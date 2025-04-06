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
Our biggest issue was solving endless elections which was a recurring theme during implementation, the deliverables have not completely solved this.

The client will not deadlock however, as there are no blocking processes. It will dispatch a message and return to waiting for input from the user.