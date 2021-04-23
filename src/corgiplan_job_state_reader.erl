-module(corgiplan_job_state_reader).

-behaviour(gen_server).

-include_lib("corgiplan_lib/include/corgiplan_job.hrl").
-include("corgiplan_execution_result.hrl").

%% API
-export([stop/1, start_link/1, get_job_state/1, get_execution_results/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {dummy}).

-spec get_job_state(JobServerName::term()) -> {ok, #corgiplan_job{}} | not_found.
get_job_state(JobServerName) ->
    gen_server:call({global, corgiplan_job_state_reader}, {get_job_state, JobServerName}).

-spec get_execution_results(JobStateReaderServerName::term(), JobServerName::term()) -> {ok, [#corgiplan_execution_result{}]}.
get_execution_results(JobStateReaderServerName, JobServerName) ->
    gen_server:call(JobStateReaderServerName, {get_execution_results, JobServerName}).

stop(Name) ->
    gen_server:call(Name, stop).

start_link(JobStateReaderServerName) ->
    gen_server:start_link(JobStateReaderServerName,
                          corgiplan_job_state_reader,
                          [],
                          []).

init(_Args) ->
    {ok, #state{dummy=1}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call({get_job_state, JobServerName}, _From, State) ->
    T = fun() ->
        mnesia:read({corgiplan_job, JobServerName})
    end,
    {atomic, Result} = mnesia:transaction(T),
    case Result of 
        [] -> {reply, not_found, State};
        [Job] -> {reply, {ok, Job}, State}
    end;
handle_call({get_execution_results, JobServerName}, _From, State) ->
    T = fun() ->
        mnesia:read({corgiplan_execution_result, JobServerName})
    end,
    {atomic, Result} = mnesia:transaction(T),
    {reply, {ok, Result}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

