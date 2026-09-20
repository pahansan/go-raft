package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

const (
	Leader = iota
	Follower
	Candidate
)

type VoteRequest struct {
	Term        int `json:"term"`
	CandidateID int `json:"CandidateID"`
}

type VoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type State struct {
	CurrentTerm int
	VotedFor    int
}

var state = State{CurrentTerm: 0, VotedFor: -1}
var role = Follower

func health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func requestVote(c *gin.Context) {
	var req VoteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Status(http.StatusInternalServerError)
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
		return
	}

	c.JSON(http.StatusOK, VoteResponse{Term: state.CurrentTerm, VoteGranted: false})
}

func main() {
	router := gin.Default()
	router.GET("/health", health)
	router.POST("/request-vote", requestVote)
	router.Run("localhost:8080")
}
