spg: Scalable Process Groups
=====

Replacement for Erlang/OTP pg2 implementation, with several restrictions.
Implements Strong Eventual Consistency (SEC).

Assumptions
-----------

Architecture
------------

Build
-----

    $ rebar3 compile

Tests
-----
Uses PropEr library to simulate all possible state changes. Unfortunately, there are bugs in OTP code
preventing using 'cover' tool with multiple slave nodes starting and shutting down rapidly, thus 
coverage reports are turned off by default.

It is however possible to run spg_SUITE with coverage turned on.


### Model
Commands:
 * nodeup/nodedown
 * connect/disconnect peer node (between any 2 nodes)
 * spawn/kill process (on any node)
 * join/leave group (any node, any process, leave can be tried even when process did not join)
 * multi-join/leave (multiple process on the same node)
 * (not implemented yet, covered with simple tests) start/stop spg scope
 
Properties:
 * process group contains only accessible pids
 * process group contains all pids that are in the same 
     group on all other dist-connected nodes

Implementation
--------------
Relies on message ordering done by Erlang distribution. All exchanges are happening only between
corresponding spg gen_server processes.

Feature requests
----------------
Next releases are going to contain:
 * customisation for which_groups() to return groups sorted (using ordered_set internally)
 * group membership changes notifications
 * (debatable) empty groups (for drop-in pg2 compatibility)
 * (debatable) concept of 'group ownership' for garbage-collecting empty groups


Changelog
---------
Version 1.0.0:
 - initial release
