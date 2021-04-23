-module(corgiplan_job_state_manager).

-behaviour(gen_server).

-include_lib("corgiplan_lib/include/corgiplan_job.hrl").

-include("corgiplan_execution_result.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export([arm_job_for_execution/2, mark_success/2, mark_max_retry_failure/2, init_job/2,
         start_link/1, stop/1]).
-export([code_change/3, handle_call/3, handle_cast/2, handle_info/2, init/1,
         terminate/2]).

stop(Name) ->
    gen_server:call(Name, stop).

start_link(JobStateManagerServerName) ->
    gen_server:start_link(JobStateManagerServerName, corgiplan_job_state_manager, [], []).

arm_job_for_execution(JobStateManagerServerName, JobState) ->
    gen_server:call(JobStateManagerServerName, {arm_job_for_execution, JobState}).

mark_success(JobStateManagerServerName, Job) ->
    gen_server:call(JobStateManagerServerName, {mark_success, Job}).

mark_max_retry_failure(JobStateManagerServerName, Job) ->
    gen_server:call(JobStateManagerServerName, {mark_max_retry_failure, Job}).

init_job(JobStateManagerServerName, InitJobState) ->
    gen_server:call(JobStateManagerServerName, {init_job, InitJobState}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_Args) ->
    {ok, empty_state}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call({init_job, JobInitState = #corgiplan_job{job_server_name = JobServerName}},
            _From,
            State) ->
    Transaction =
        fun() ->
           case mnesia:read({corgiplan_job, JobServerName}) of
               [] ->
                   ok = mnesia:write(JobInitState),
                   JobInitState;
               [CurrentJobState] ->
                   CurrentJobState
           end
        end,
    {atomic, JobState} = mnesia:transaction(Transaction),
    {reply, {ok, JobState}, State};
handle_call({arm_job_for_execution, Job0}, _From, State) ->
    Transaction =
        fun() ->
           ok = mnesia:write(Job0),
           Job0
        end,
    {atomic, Job} = mnesia:transaction(Transaction),
    {reply, {ok, Job}, State};
handle_call({mark_success, Job}, _From, State) ->
    ok = write_result(Job, success),
    {reply, ok, State};
handle_call({mark_max_retry_failure, Job}, _From, State) ->
    ok = write_result(Job, max_retry_failure),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec write_result(Job :: #corgiplan_job{}, Result :: term) -> ok.
write_result(Job, Result) ->
    Transaction =
        fun() ->
           ExecutionResult = get_execution_result(Job, Result),
           mnesia:write(ExecutionResult)
        end,
    {atomic, ok} = mnesia:transaction(Transaction),
    ok.

-spec get_execution_result(Job :: #corgiplan_job{},
                           Result :: success | marked_as_failure | max_retry_failure) ->
                              #corgiplan_execution_result{}.
get_execution_result(#corgiplan_job{job_server_name = JobServerName,
                                    armed_execution_time = ArmedExecutionTime},
                     Result) ->
    #corgiplan_execution_result{job_server_name = JobServerName,
                                execution_time = ArmedExecutionTime,
                                status = Result}.
