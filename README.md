# Implementing the Raft consensus algorithm in Go.
***Programming Assignment 3 "Distributed Consensus"*** \
*Subject: CADP - Samhliða og dreifð forritun* \
*Instructor: Marcel Kyaz*

## Authors: 
*Hlynur Ísak Vilmundarson* \
*Kacper Kaczynski*

---
#### Documentation
There are pointers to the relevant sections in the Raft paper "In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout as code comments. However, the code is not really completely functional in its current state which made it hard to describe its actual correctness in relation to the paper. 
Our biggest issue was solving endless elections which was a recurring theme during implementation that lead to our demise (i.e. we gave up on this).

The client will not deadlock however, as there are no blocking processes. It will dispatch a message and return to waiting for input from the user.

The heartbeat is sent out by the leader periodically every 50 ms. The elecetion timeout for followers is between 150ms and 300ms, giving plenty of time for them to reset their timers between heartbeats if everything goes according to plan. If the heartbeat is not received by some follower before its randomized timeout within the interval it will request an election as per the paper specification.