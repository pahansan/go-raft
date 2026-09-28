package main

import (
	"bytes"
	"context"
	"encoding/json"
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
)

type StatusRequest struct {
	// empty
}

type StatusResponse struct {
	Term int    `json:"term"`
	Role string `json:"role"`
	ID   int    `json:"id"`
}

type VoteRequest struct {
	Term        int `json:"term"`
	CandidateID int `json:"candidateID"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesRequest struct {
	Term     int `json:"term"`
	LeaderID int `json:"leaderID"`
}

type AppendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}

type State struct {
	mu          sync.Mutex
	role        int
	currentTerm int
	votedFor    int

	id         int
	otherIDs   []int
	otherHosts []string

	gotRPC chan struct{}
}

func NewState(id int, hosts []string) *State {
	s := &State{
		id:         id,
		otherHosts: hosts,
		votedFor:   -1,
		gotRPC:     make(chan struct{}, 1),
	}
	for _, host := range hosts {
		s.otherIDs = append(s.otherIDs, getPort(host))
	}
	return s
}

func (s *State) handleStatus() StatusResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StatusResponse{Term: s.currentTerm, Role: RoleAsString(s.role), ID: s.id}
}

func (s *State) handleVote(req VoteRequest) VoteResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Term < s.currentTerm {
		printLog(s.currentTerm, "Rejected vote for "+strconv.Itoa(req.CandidateID)+": candidate term "+strconv.Itoa(req.Term)+" < current term")
		return VoteResponse{Term: s.currentTerm, VoteGranted: false}
	}

	if req.Term > s.currentTerm {
		oldRole := s.role
		s.currentTerm = req.Term
		s.votedFor = -1
		s.role = Follower
		printLog(s.currentTerm, "Role "+RoleAsString(oldRole)+"->"+RoleAsString(Follower)+": RequestVote from "+strconv.Itoa(req.CandidateID)+" in higher term")
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

	if req.Term < s.currentTerm {
		printLog(s.currentTerm, "Rejected AppendEntries from leader "+strconv.Itoa(req.LeaderID)+": leader term "+strconv.Itoa(req.Term)+" < current term")
		return AppendEntriesResponse{Term: s.currentTerm, Success: false}
	}

	oldRole := s.role
	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.votedFor = -1
		printLog(s.currentTerm, "Reset votedFor: higher term in AppendEntries from leader "+strconv.Itoa(req.LeaderID))
	}
	s.role = Follower

	if oldRole != Follower {
		printLog(s.currentTerm, "Role "+RoleAsString(oldRole)+"->"+RoleAsString(Follower)+": AppendEntries from leader "+strconv.Itoa(req.LeaderID))
	} else {
		printLog(s.currentTerm, "Accepted AppendEntries from leader "+strconv.Itoa(req.LeaderID))
	}

	return AppendEntriesResponse{Term: s.currentTerm, Success: true}
}

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

func (s *State) appendEntries(c *gin.Context) {
	var req AppendEntriesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	resp := s.handleAppend(req)
	c.JSON(http.StatusOK, resp)

	s.notifyRPC()
}

func (s *State) notifyRPC() {
	select {
	case s.gotRPC <- struct{}{}:
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
	printLog(s.currentTerm, "Role "+RoleAsString(oldRole)+"->"+RoleAsString(Follower)+": "+reason)
}

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
	term := s.currentTerm
	s.mu.Unlock()

	printLog(term, "Started election|Role "+RoleAsString(oldRole)+"->"+RoleAsString(Candidate))

	body, err := json.Marshal(VoteRequest{Term: term, CandidateID: s.id})
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

			req, err := http.NewRequest("POST", "http://"+host+"/request-vote", bytes.NewBuffer(body))
			if err != nil {
				return
			}

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
	defer s.mu.Unlock()

	if s.role != Candidate || s.currentTerm != term {
		printLog(s.currentTerm, "Election for term "+strconv.Itoa(term)+" aborted: role "+RoleAsString(s.role)+", current term "+strconv.Itoa(s.currentTerm))
		return
	}

	total := len(s.otherIDs) + 1
	needed := total/2 + 1
	if votes < needed {
		printLog(s.currentTerm, "Election for term "+strconv.Itoa(term)+" lost: "+strconv.Itoa(votes)+"/"+strconv.Itoa(total)+" votes, need "+strconv.Itoa(needed))
		return
	}

	printLog(s.currentTerm, "Role "+RoleAsString(s.role)+"->"+RoleAsString(Leader)+": won election with "+strconv.Itoa(votes)+"/"+strconv.Itoa(total)+" votes")
	s.role = Leader
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

func (s *State) sendHeartbeat() {
	s.mu.Lock()
	if s.role != Leader {
		s.mu.Unlock()
		return
	}
	term := s.currentTerm
	s.mu.Unlock()

	body, err := json.Marshal(AppendEntriesRequest{Term: term, LeaderID: s.id})
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	for _, host := range s.otherHosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, "POST", "http://"+host+"/append-entries", bytes.NewBuffer(body))
			if err != nil {
				return
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var respBody AppendEntriesResponse
			if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
				return
			}

			if respBody.Term > term {
				s.stepDown(respBody.Term, "higher term in heartbeat response from "+host)
				return
			}
		}(host)
	}
	wg.Wait()
}

func (s *State) leaderLoop() {
	for range time.Tick(heartbeatTick * time.Millisecond) {
		s.sendHeartbeat()
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
	router.Run(addr)
}
