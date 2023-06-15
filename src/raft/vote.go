package raft

import (
	"log"
	"math/rand"
	"time"
)

const electionTimeout = 50 * time.Millisecond

func (rf *Raft) requestVoteL() {
	args := &RequestVoteArgs{}
	votesChan := make(chan bool)
	done := make(chan bool)
	votes := 1
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := &RequestVoteReply{}
				votesChan <- rf.sendRequestVote(i, args, reply)
			}(i)
		}
		if i == len(rf.peers) {
			done <- true
		}
	}
	select {
	case isAgree := <-votesChan:
		if isAgree {
			votes++
		}
	case <-done:
		log.Println("vote done")
	}
}

func (rf *Raft) startElectionL() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()

	DPrintf("%v: start election term %v\n", rf.me, rf.currentTerm)

	rf.requestVoteL()
}

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v: tick state %v\n", rf.me, rf.state)

	if rf.state == Leader {
		rf.setElectionTime()
		// send append entries req
	}

	if time.Now().After(rf.electionTime) {
		rf.setElectionTime()
		// start election as candidate
		rf.startElectionL()
	}
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(electionTimeout)
	ms := rand.Int63() % 300
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		for rf.killed() == false {
			rf.tick()
			//check every 50ms
			// ms := 50 + (rand.Int63() % 300)
			ms := 50
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
	}
}
