package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "go-raft/docs"
)

const (
	Follower = iota
	Candidate
	Leader
)

const (
	electionTimeoutFrom = 300
	electionTimeoutTo   = 600
	heartbeatTick       = 100

	rpcTimeout    = 100 * time.Millisecond
	clientTimeout = 5 * time.Second
)

type StatusRequest struct {
}

type StatusResponse struct {
	Term int    `json:"term"`
	Role string `json:"role"`
	ID   int    `json:"id"`
}

type VoteRequest struct {
	Term         int `json:"term"`
	CandidateID  int `json:"candidateID"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type LogEntry struct {
	Index   int `json:"index"`
	Term    int `json:"term"`
	Command any `json:"command"`
}

type AppendEntriesRequest struct {
	Term         int        `json:"term"`
	LeaderID     int        `json:"leaderID"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type AddEntriesRequest struct {
	Command any   `json:"command"`
	Entries []any `json:"entries"`
}

type AddEntriesResponse struct {
	Indexes    []int  `json:"indexes,omitempty"`
	Term       int    `json:"term"`
	Committed  bool   `json:"committed"`
	Error      string `json:"error,omitempty"`
	LeaderID   int    `json:"leaderID,omitempty"`
	LeaderHost string `json:"leaderHost,omitempty"`
}

var (
	errNotLeader         = errors.New("not leader")
	errLeadershipChanged = errors.New("leadership changed before entries were committed")
	errCommitTimeout     = errors.New("timed out waiting for entries to commit")
)

func (r AddEntriesRequest) commands() []any {
	commands := make([]any, 0, len(r.Entries)+1)
	if r.Command != nil {
		commands = append(commands, r.Command)
	}
	return append(commands, r.Entries...)
}

type LogEntryView struct {
	Index     int  `json:"index"`
	Term      int  `json:"term"`
	Command   any  `json:"command"`
	Committed bool `json:"committed"`
	Applied   bool `json:"applied"`
}

type ShowLogResponse struct {
	ID          int            `json:"id"`
	Term        int            `json:"term"`
	Role        string         `json:"role"`
	CommitIndex int            `json:"commitIndex"`
	LastApplied int            `json:"lastApplied"`
	Entries     []LogEntryView `json:"entries"`
}

type State struct {
	mu          sync.Mutex
	role        int
	currentTerm int
	votedFor    int

	id         int
	otherIDs   []int
	otherHosts []string

	leaderID int

	log []LogEntry

	commitIndex int
	lastApplied int

	stateMachine []LogEntry

	nextIndex  map[int]int
	matchIndex map[int]int

	gotRPC      chan struct{}
	replicateCh chan struct{}
	stateCh     chan struct{}
}

func NewState(id int, hosts []string) *State {
	s := &State{
		id:          id,
		otherHosts:  hosts,
		votedFor:    -1,
		leaderID:    -1,
		log:         []LogEntry{{Index: 0, Term: 0}},
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		gotRPC:      make(chan struct{}, 1),
		replicateCh: make(chan struct{}, 1),
		stateCh:     make(chan struct{}),
	}
	for _, host := range hosts {
		s.otherIDs = append(s.otherIDs, getPort(host))
	}
	return s
}

func (s *State) lastLogIndexLocked() int {
	return len(s.log) - 1
}

func (s *State) lastLogTermLocked() int {
	return s.log[s.lastLogIndexLocked()].Term
}

func (s *State) termAtLocked(index int) int {
	return s.log[index].Term
}

func (s *State) entriesFromLocked(index int) []LogEntry {
	entries := make([]LogEntry, len(s.log)-index)
	copy(entries, s.log[index:])
	return entries
}

func (s *State) notifyStateLocked() {
	close(s.stateCh)
	s.stateCh = make(chan struct{})
}

func (s *State) handleStatus() StatusResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StatusResponse{Term: s.currentTerm, Role: RoleAsString(s.role), ID: s.id}
}

func (s *State) handleVote(req VoteRequest) VoteResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldRole := s.role
	oldTerm := s.currentTerm
	defer func() {
		if oldRole != s.role || oldTerm != s.currentTerm {
			s.notifyStateLocked()
		}
	}()

	if req.Term < s.currentTerm {
		printLog(s.currentTerm, "Rejected vote for "+strconv.Itoa(req.CandidateID)+": candidate term "+strconv.Itoa(req.Term)+" < current term")
		return VoteResponse{Term: s.currentTerm, VoteGranted: false}
	}

	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.votedFor = -1
		s.role = Follower
		s.leaderID = -1
		printLog(s.currentTerm, "Role "+RoleAsString(oldRole)+"->"+RoleAsString(Follower)+": RequestVote from "+strconv.Itoa(req.CandidateID)+" in higher term")
	}

	lastLogIndex := s.lastLogIndexLocked()
	lastLogTerm := s.lastLogTermLocked()
	upToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)
	if !upToDate {
		printLog(s.currentTerm, "Rejected vote for "+strconv.Itoa(req.CandidateID)+": candidate log ("+
			strconv.Itoa(req.LastLogIndex)+"/"+strconv.Itoa(req.LastLogTerm)+") is less up-to-date than ours ("+
			strconv.Itoa(lastLogIndex)+"/"+strconv.Itoa(lastLogTerm)+")")
		return VoteResponse{Term: s.currentTerm, VoteGranted: false}
	}

	if s.votedFor == -1 || s.votedFor == req.CandidateID {
		s.votedFor = req.CandidateID
		printLog(s.currentTerm, "Granted vote to "+strconv.Itoa(req.CandidateID))
		return VoteResponse{Term: s.currentTerm, VoteGranted: true}
	}

	printLog(s.currentTerm, "Rejected vote for "+strconv.Itoa(req.CandidateID)+": already voted for "+strconv.Itoa(s.votedFor))
	return VoteResponse{Term: s.currentTerm, VoteGranted: false}
}

func (s *State) handleAppend(req AppendEntriesRequest) AppendEntriesResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldRole := s.role
	oldTerm := s.currentTerm
	defer func() {
		if oldRole != s.role || oldTerm != s.currentTerm {
			s.notifyStateLocked()
		}
	}()

	if req.Term < s.currentTerm {
		printLog(s.currentTerm, "Rejected AppendEntries from leader "+strconv.Itoa(req.LeaderID)+": leader term "+strconv.Itoa(req.Term)+" < current term")
		return AppendEntriesResponse{Term: s.currentTerm, Success: false}
	}

	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.votedFor = -1
		printLog(s.currentTerm, "Reset votedFor: higher term in AppendEntries from leader "+strconv.Itoa(req.LeaderID))
	}
	s.role = Follower
	s.leaderID = req.LeaderID

	if oldRole != Follower {
		printLog(s.currentTerm, "Role "+RoleAsString(oldRole)+"->"+RoleAsString(Follower)+": AppendEntries from leader "+strconv.Itoa(req.LeaderID))
	} else {
		printLog(s.currentTerm, "Accepted AppendEntries from leader "+strconv.Itoa(req.LeaderID))
	}

	if req.PrevLogIndex < 0 || req.PrevLogIndex > s.lastLogIndexLocked() {
		printLog(s.currentTerm, "Rejected AppendEntries: prevLogIndex "+strconv.Itoa(req.PrevLogIndex)+" is outside local log (last index "+strconv.Itoa(s.lastLogIndexLocked())+")")
		return AppendEntriesResponse{Term: s.currentTerm, Success: false}
	}
	if s.termAtLocked(req.PrevLogIndex) != req.PrevLogTerm {
		printLog(s.currentTerm, "Rejected AppendEntries: no entry at index "+strconv.Itoa(req.PrevLogIndex)+" with term "+strconv.Itoa(req.PrevLogTerm))
		return AppendEntriesResponse{Term: s.currentTerm, Success: false}
	}

	for i := range req.Entries {
		req.Entries[i].Index = req.PrevLogIndex + 1 + i
	}

	appendFrom := -1
	for i := range req.Entries {
		index := req.Entries[i].Index
		if index > s.lastLogIndexLocked() {
			appendFrom = i
			break
		}
		if s.termAtLocked(index) != req.Entries[i].Term {
			appendFrom = i
			break
		}
	}

	if appendFrom >= 0 {
		index := req.PrevLogIndex + 1 + appendFrom
		if index <= s.lastLogIndexLocked() {
			printLog(s.currentTerm, "Truncated log from index "+strconv.Itoa(index)+": conflicting entry from leader "+strconv.Itoa(req.LeaderID))
			s.log = s.log[:index]
		}
		s.log = append(s.log, req.Entries[appendFrom:]...)
		printLog(s.currentTerm, "Appended "+strconv.Itoa(len(req.Entries)-appendFrom)+" entries from leader "+strconv.Itoa(req.LeaderID)+" (last index "+strconv.Itoa(s.lastLogIndexLocked())+")")
	}

	lastNew := req.PrevLogIndex + len(req.Entries)
	newCommit := req.LeaderCommit
	if newCommit > lastNew {
		newCommit = lastNew
	}
	if newCommit > s.commitIndex {
		s.commitIndex = newCommit
		s.applyLocked()
	}

	return AppendEntriesResponse{Term: s.currentTerm, Success: true}
}

func (s *State) applyLocked() {
	applied := false
	for s.commitIndex > s.lastApplied {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.stateMachine = append(s.stateMachine, entry)
		applied = true
		printLog(entry.Term, "Applied log["+strconv.Itoa(entry.Index)+"] to state machine")
	}
	if applied {
		s.notifyStateLocked()
	}
}

func (s *State) advanceCommitLocked() {
	if s.role != Leader {
		return
	}

	total := len(s.otherIDs) + 1
	for n := s.lastLogIndexLocked(); n > s.commitIndex; n-- {
		if s.termAtLocked(n) != s.currentTerm {
			continue
		}

		count := 1
		for _, id := range s.otherIDs {
			if s.matchIndex[id] >= n {
				count++
			}
		}
		if count*2 > total {
			s.commitIndex = n
			printLog(s.currentTerm, "Committed up to index "+strconv.Itoa(n)+" ("+strconv.Itoa(count)+"/"+strconv.Itoa(total)+" replicas)")
			s.applyLocked()
			return
		}
	}
}

func (s *State) entryAppliedLocked(index, term int) bool {
	return s.lastApplied >= index && index < len(s.log) && s.log[index].Term == term
}

func (s *State) waitApplied(index, term int, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		s.mu.Lock()
		if s.lastApplied >= index {
			applied := s.entryAppliedLocked(index, term)
			s.mu.Unlock()
			return applied
		}
		if s.role != Leader || s.currentTerm != term {
			s.mu.Unlock()
			return false
		}
		changed := s.stateCh
		s.mu.Unlock()

		select {
		case <-changed:
		case <-timer.C:
			s.mu.Lock()
			applied := s.entryAppliedLocked(index, term)
			s.mu.Unlock()
			return applied
		}
	}
}

func (s *State) hostForIDLocked(id int) string {
	for i, otherID := range s.otherIDs {
		if otherID == id {
			return s.otherHosts[i]
		}
	}
	return ""
}

// @Summary      RequestVote RPC
// @Description  Кандидат запрашивает голос у узла.
// @Tags         raft
// @Accept       json
// @Produce      json
// @Param        request  body      VoteRequest  true  "Запрос голоса"
// @Success      200      {object}  VoteResponse
// @Failure      400      {string}  string       "Некорректный JSON"
// @Router       /request-vote [post]
func (s *State) requestVote(c *gin.Context) {
	var req VoteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	resp := s.handleVote(req)
	c.JSON(http.StatusOK, resp)

	if resp.VoteGranted {
		s.notifyRPC()
	}
}

// @Summary      AppendEntries RPC
// @Description  Лидер реплицирует записи лога и передаёт commitIndex, либо heartbeat.
// @Tags         raft
// @Accept       json
// @Produce      json
// @Param        request  body      AppendEntriesRequest  true  "Запрос репликации"
// @Success      200      {object}  AppendEntriesResponse
// @Failure      400      {string}  string                "Некорректный JSON"
// @Router       /append-entries [post]
func (s *State) appendEntries(c *gin.Context) {
	var req AppendEntriesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	resp := s.handleAppend(req)
	c.JSON(http.StatusOK, resp)

	if req.Term >= resp.Term {
		s.notifyRPC()
	}
}

func (s *State) appendCommands(commands []any) ([]int, int, error) {
	s.mu.Lock()
	term := s.currentTerm
	if s.role != Leader {
		s.mu.Unlock()
		return nil, term, errNotLeader
	}

	indexes := make([]int, 0, len(commands))
	for _, command := range commands {
		index := s.lastLogIndexLocked() + 1
		s.log = append(s.log, LogEntry{Index: index, Term: term, Command: command})
		indexes = append(indexes, index)
	}
	targetIndex := indexes[len(indexes)-1]

	s.advanceCommitLocked()
	s.mu.Unlock()

	s.triggerReplication()

	if s.waitApplied(targetIndex, term, clientTimeout) {
		return indexes, term, nil
	}

	s.mu.Lock()
	role := s.role
	s.mu.Unlock()
	if role != Leader {
		return indexes, term, errLeadershipChanged
	}
	return indexes, term, errCommitTimeout
}

func (s *State) leaderInfo() (int, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leaderID, s.hostForIDLocked(s.leaderID)
}

// @Summary      Добавить записи в лог
// @Tags         client
// @Accept       json
// @Produce      json
// @Param        request  body      AddEntriesRequest  true  "Команда или пакет команд"
// @Success      200      {object}  AddEntriesResponse
// @Failure      400      {object}  AddEntriesResponse
// @Failure      409      {object}  AddEntriesResponse
// @Failure      504      {object}  AddEntriesResponse
// @Header       409      {string}  Location  "http://<leader>/add-entries"
// @Router       /add-entries [post]
func (s *State) addEntries(c *gin.Context) {
	var req AddEntriesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, AddEntriesResponse{Error: "invalid JSON"})
		return
	}

	commands := req.commands()
	if len(commands) == 0 {
		c.JSON(http.StatusBadRequest, AddEntriesResponse{Error: "command or entries is required"})
		return
	}

	indexes, term, err := s.appendCommands(commands)
	if err == nil {
		c.JSON(http.StatusOK, AddEntriesResponse{Indexes: indexes, Term: term, Committed: true})
		return
	}

	resp := AddEntriesResponse{Indexes: indexes, Term: term, Error: err.Error()}
	resp.LeaderID, resp.LeaderHost = s.leaderInfo()

	if err == errNotLeader {
		if resp.LeaderHost != "" {
			c.Header("Location", "http://"+resp.LeaderHost+"/add-entries")
		}
		c.JSON(http.StatusConflict, resp)
		return
	}
	if err == errLeadershipChanged {
		c.JSON(http.StatusConflict, resp)
		return
	}
	c.JSON(http.StatusGatewayTimeout, resp)
}

// @Summary      Показать лог узла
// @Tags         client
// @Produce      json
// @Success      200  {object}  ShowLogResponse
// @Router       /show-log [get]
func (s *State) showLog(c *gin.Context) {
	s.mu.Lock()
	entries := make([]LogEntryView, 0, s.lastLogIndexLocked())
	for i := 1; i < len(s.log); i++ {
		entry := s.log[i]
		entries = append(entries, LogEntryView{
			Index:     entry.Index,
			Term:      entry.Term,
			Command:   entry.Command,
			Committed: i <= s.commitIndex,
			Applied:   i <= s.lastApplied,
		})
	}
	resp := ShowLogResponse{
		ID:          s.id,
		Term:        s.currentTerm,
		Role:        RoleAsString(s.role),
		CommitIndex: s.commitIndex,
		LastApplied: s.lastApplied,
		Entries:     entries,
	}
	s.mu.Unlock()

	c.JSON(http.StatusOK, resp)
}

func (s *State) notifyRPC() {
	select {
	case s.gotRPC <- struct{}{}:
	default:
	}
}

func (s *State) triggerReplication() {
	select {
	case s.replicateCh <- struct{}{}:
	default:
	}
}

func (s *State) stepDown(term int, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if term <= s.currentTerm {
		return
	}

	oldRole := s.role
	s.currentTerm = term
	s.votedFor = -1
	s.role = Follower
	s.leaderID = -1
	printLog(s.currentTerm, "Role "+RoleAsString(oldRole)+"->"+RoleAsString(Follower)+": "+reason)
	s.notifyStateLocked()
}

// @Summary      Статус узла
// @Tags         cluster
// @Produce      json
// @Success      200  {object}  StatusResponse
// @Router       /status [get]
func (s *State) status(c *gin.Context) {
	c.JSON(http.StatusOK, s.handleStatus())
}

func electionTimeout() time.Duration {
	return time.Duration((electionTimeoutFrom + rand.IntN(electionTimeoutTo-electionTimeoutFrom))) * time.Millisecond
}

func printLog(term int, m string) {
	fmt.Println("[Term: " + strconv.Itoa(term) + "] " + m)
}

func RoleAsString(role int) string {
	switch role {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return "nil"
}

func (s *State) startElection() {
	s.mu.Lock()
	if s.role == Leader {
		s.mu.Unlock()
		return
	}
	oldRole := s.role
	s.role = Candidate
	s.currentTerm++
	s.votedFor = s.id
	s.leaderID = -1
	term := s.currentTerm
	lastLogIndex := s.lastLogIndexLocked()
	lastLogTerm := s.lastLogTermLocked()
	s.notifyStateLocked()
	s.mu.Unlock()

	printLog(term, "Started election|Role "+RoleAsString(oldRole)+"->"+RoleAsString(Candidate))

	body, err := json.Marshal(VoteRequest{
		Term:         term,
		CandidateID:  s.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		log.Fatal(err)
	}

	var votes atomic.Int64
	votes.Store(1)

	var wg sync.WaitGroup
	for _, host := range s.otherHosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+host+"/request-vote", bytes.NewBuffer(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var respBody VoteResponse
			if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
				return
			}

			if respBody.Term > term {
				s.stepDown(respBody.Term, "higher term in vote response from "+host)
				return
			}
			if respBody.VoteGranted {
				votes.Add(1)
				printLog(term, "Vote granted by "+host)
			} else {
				printLog(term, "Vote denied by "+host)
			}
		}(host)
	}
	wg.Wait()

	s.becomeLeaderIfWon(term, int(votes.Load()))
}

func (s *State) becomeLeaderIfWon(term, votes int) {
	s.mu.Lock()

	if s.role != Candidate || s.currentTerm != term {
		printLog(s.currentTerm, "Election for term "+strconv.Itoa(term)+" aborted: role "+RoleAsString(s.role)+", current term "+strconv.Itoa(s.currentTerm))
		s.mu.Unlock()
		return
	}

	total := len(s.otherIDs) + 1
	needed := total/2 + 1
	if votes < needed {
		printLog(s.currentTerm, "Election for term "+strconv.Itoa(term)+" lost: "+strconv.Itoa(votes)+"/"+strconv.Itoa(total)+" votes, need "+strconv.Itoa(needed))
		s.mu.Unlock()
		return
	}

	printLog(s.currentTerm, "Role "+RoleAsString(s.role)+"->"+RoleAsString(Leader)+": won election with "+strconv.Itoa(votes)+"/"+strconv.Itoa(total)+" votes")
	s.role = Leader
	s.leaderID = s.id

	s.nextIndex = make(map[int]int, len(s.otherIDs))
	s.matchIndex = make(map[int]int, len(s.otherIDs))
	next := s.lastLogIndexLocked() + 1
	for _, id := range s.otherIDs {
		s.nextIndex[id] = next
		s.matchIndex[id] = 0
	}
	s.notifyStateLocked()
	s.mu.Unlock()

	s.triggerReplication()
}

func (s *State) electionLoop() {
	timer := time.NewTimer(electionTimeout())

	for {
		select {
		case <-timer.C:
			s.startElection()
			timer.Reset(electionTimeout())
		case <-s.gotRPC:
			timer.Reset(electionTimeout())
		}
	}
}

func (s *State) replicateAll() {
	s.mu.Lock()
	if s.role != Leader {
		s.mu.Unlock()
		return
	}
	hosts := append([]string(nil), s.otherHosts...)
	ids := append([]int(nil), s.otherIDs...)
	s.mu.Unlock()

	var wg sync.WaitGroup
	for i := range hosts {
		wg.Add(1)
		go func(id int, host string) {
			defer wg.Done()
			s.replicatePeer(id, host)
		}(ids[i], hosts[i])
	}
	wg.Wait()
}

func (s *State) replicatePeer(peerID int, host string) {
	for {
		s.mu.Lock()
		if s.role != Leader {
			s.mu.Unlock()
			return
		}
		term := s.currentTerm
		next := s.nextIndex[peerID]
		prevLogIndex := next - 1
		prevLogTerm := s.termAtLocked(prevLogIndex)
		entries := s.entriesFromLocked(next)
		leaderCommit := s.commitIndex
		s.mu.Unlock()

		req := AppendEntriesRequest{
			Term:         term,
			LeaderID:     s.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		resp, err := s.postAppend(host, req)
		if err != nil {
			return
		}

		if resp.Term > term {
			s.stepDown(resp.Term, "higher term in AppendEntries response from "+host)
			return
		}

		s.mu.Lock()
		if s.role != Leader || s.currentTerm != term {
			s.mu.Unlock()
			return
		}

		if resp.Success {
			s.nextIndex[peerID] = prevLogIndex + len(entries) + 1
			s.matchIndex[peerID] = prevLogIndex + len(entries)
			s.advanceCommitLocked()
			s.mu.Unlock()
			return
		}

		current := s.nextIndex[peerID]
		if current <= 1 {
			s.mu.Unlock()
			return
		}
		s.nextIndex[peerID] = current - 1
		s.mu.Unlock()
	}
}

func (s *State) postAppend(host string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var respBody AppendEntriesResponse

	body, err := json.Marshal(req)
	if err != nil {
		return respBody, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+host+"/append-entries", bytes.NewBuffer(body))
	if err != nil {
		return respBody, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return respBody, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return respBody, fmt.Errorf("append-entries to %s returned %s", host, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		return respBody, err
	}
	return respBody, nil
}

func (s *State) leaderLoop() {
	ticker := time.NewTicker(heartbeatTick * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.replicateAll()
		case <-s.replicateCh:
			s.replicateAll()
		}
	}
}

func getPort(addr string) int {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatal(err)
	}
	id, err := strconv.Atoi(port)
	if err != nil {
		log.Fatal(err)
	}
	return id
}

// @title           Go Raft API
// @version         1.0
// @BasePath        /
func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: " + os.Args[0] + " <host>:<port> [<host>:<port> ...]")
	}
	nodes := os.Args[1:]
	addr := nodes[0]
	s := NewState(getPort(addr), nodes[1:])

	go s.electionLoop()
	go s.leaderLoop()

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/status", s.status)
	router.POST("/request-vote", s.requestVote)
	router.POST("/append-entries", s.appendEntries)
	router.POST("/add-entries", s.addEntries)
	router.GET("/show-log", s.showLog)

	router.GET("/swagger", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/swagger/index.html")
	})
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	router.Run(addr)
}
