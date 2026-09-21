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
	"time"

	"github.com/gin-gonic/gin"
)

const (
	Leader = iota
	Follower
	Candidate
)

const (
	electionTimeoutFrom = 300
	electionTimeoutTo   = 600
	heartbeatTick       = 100
)

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
	CurrentTerm int
	VotedFor    int
}

var state = State{CurrentTerm: 0, VotedFor: -1}
var role = Follower
var ID = -1
var otherIDs = []int{}
var otherHosts = []string{}
var gotRPC = make(chan struct{}, 1)

func health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func requestVote(c *gin.Context) {
	var req VoteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	if req.Term < state.CurrentTerm {
		c.JSON(http.StatusOK, VoteResponse{Term: state.CurrentTerm, VoteGranted: false})
		return
	}

	if req.Term > state.CurrentTerm {
		state.CurrentTerm = req.Term
		state.VotedFor = -1
		role = Follower
	}

	if state.VotedFor == -1 || state.VotedFor == req.CandidateID {
		state.VotedFor = req.CandidateID
		c.JSON(http.StatusOK, VoteResponse{Term: state.CurrentTerm, VoteGranted: true})
		fmt.Println("Voted for " + strconv.Itoa(state.VotedFor))
		gotRPC <- struct{}{}
		return
	}

	c.JSON(http.StatusOK, VoteResponse{Term: state.CurrentTerm, VoteGranted: false})
}

func appendEntries(c *gin.Context) {
	var req AppendEntriesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	if req.Term < state.CurrentTerm {
		c.JSON(http.StatusOK, AppendEntriesResponse{Term: state.CurrentTerm, Success: false})
		return
	}

	if req.Term > state.CurrentTerm {
		state.CurrentTerm = req.Term
	}

	role = Follower

	c.JSON(http.StatusOK, AppendEntriesResponse{Term: state.CurrentTerm, Success: true})
	gotRPC <- struct{}{}
}

func electionTimeout() time.Duration {
	return time.Duration((electionTimeoutFrom + rand.IntN(electionTimeoutTo-electionTimeoutFrom))) * time.Millisecond
}

func startElection() {
	fmt.Println("Started election")
	role = Candidate
	state.CurrentTerm++
	state.VotedFor = ID
	body := VoteRequest{Term: state.CurrentTerm, CandidateID: ID}
	out, err := json.Marshal(body)
	if err != nil {
		log.Fatal(err)
	}
	votes := 1

	var wg sync.WaitGroup
	for i := range otherIDs {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req, err := http.NewRequest("POST", "http://"+otherHosts[i]+"/request-vote", bytes.NewBuffer(out))
			if err != nil {
				log.Fatal(err)
			}

			req = req.WithContext(context.Background())
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var respBody VoteResponse
			if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
				return
			}
			if respBody.VoteGranted {
				votes++
			}
		}(i)
	}
	wg.Wait()
	if votes > len(otherIDs)/2 {
		role = Leader
	}
}

func electionLoop() {
	timer := time.NewTimer(electionTimeout())
	for {
		select {
		case <-timer.C:
			if role != Leader {
				startElection()
			}
			timer.Reset(electionTimeout())
		case <-gotRPC:
			timer.Reset(electionTimeout())
		}
	}
}

func leaderLoop() {
	for range time.Tick(heartbeatTick * time.Millisecond) {
		if role != Leader {
			continue
		}
		body := AppendEntriesRequest{Term: state.CurrentTerm, LeaderID: ID}
		out, err := json.Marshal(body)
		if err != nil {
			log.Fatal(err)
		}

		var wg sync.WaitGroup
		for i := range otherIDs {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				req, err := http.NewRequest("POST", "http://"+otherHosts[i]+"/append-entries", bytes.NewBuffer(out))
				if err != nil {
					log.Fatal(err)
				}

				req = req.WithContext(context.Background())
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return
				}
				defer resp.Body.Close()
			}(i)
		}
		wg.Wait()

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
	ID = getPort(addr)
	otherHosts = nodes[1:]
	for _, host := range otherHosts {
		otherIDs = append(otherIDs, getPort(host))
	}
	go electionLoop()
	go leaderLoop()
	router := gin.Default()
	router.GET("/health", health)
	router.POST("/request-vote", requestVote)
	router.POST("/append-entries", appendEntries)
	router.Run(addr)
}
