corgiplan_lib
=====

An OTP library

A library with subroutines for distributed scheduling system. This is a proof of concept project. Certain design limitations are present. The principal goal is to have something better than Apache Airflow.

Compared to Airflow the following differences are proposed:

* each "DAG" is acutally a live running process or a program
* each "DAG" process contains its own scheduler
* the "operators" are also separate processes/programs that can have non-hetorogenic states

Build
-----

    $ rebar3 compile
