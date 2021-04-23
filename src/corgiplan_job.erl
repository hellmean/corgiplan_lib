-module(corgiplan_job).

-behaviour(gen_server).

-include("corgiplan_job.hrl").

%% callbacks

-callback cooldown_millis() -> non_neg_integer().
-callback inception_time() -> calendar:datetime1970().
-callback crontab_schedule() -> term().
%if we return fail this stops execution without retries.
-callback max_retries() -> non_neg_integer().
-callback execution_plan(ExecutionTimeUTC :: calendar:datetime1970()) -> ok | fail.

-export([register/1, stop/1, start_link/2, run_parallel/1, make_init_job_state/1,
         get_next_execution_time/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% API

% -spec get_crontab_schedule(JobServerName :: term()) -> term().
% get_crontab_schedule(JobServerName) ->
%     {ok, Result} = gen_server:call(JobServerName, get_crontab_schedule),
%     Result.

% -spec get_max_retries(JobServerName :: term()) -> non_neg_integer().
% get_max_retries(JobServerName) ->
%     {ok, Result} = gen_server:call(JobServerName, get_max_retries),
%     Result.

% -spec get_inception_time(JobServerName :: term()) -> calendar:datetime1970().
% get_inception_time(JobServerName) ->
%     {ok, Result} = gen_server:call(JobServerName, get_inception_time),
%     Result.

-spec get_next_execution_time(Job :: #corgiplan_job{}) -> calendar:datetime1970().
get_next_execution_time(#corgiplan_job{job_server_name = JobServerName,
                                       armed_execution_time = CurrentExecutionTimeUTC}) ->
    ParsedCrontabSpec =
        cron_ops:time_specs(
            JobServerName:crontab_schedule()),
    cron_ops:next(CurrentExecutionTimeUTC, ParsedCrontabSpec).

-spec make_init_job_state(JobServerName :: term()) -> #corgiplan_job{}.
make_init_job_state(JobServerName) ->
    InceptionTime = JobServerName:inception_time(),
    MaxRetries = JobServerName:max_retries(),
    JobAtInception =
        #corgiplan_job{job_server_name = JobServerName,
                       armed_execution_time = InceptionTime,
                       current_attempts_count = MaxRetries + 1},
    NextExecutionTimeUTC = get_next_execution_time(JobAtInception),
    JobAtInception#corgiplan_job{armed_execution_time = NextExecutionTimeUTC}.

% -spec max_retries(Job :: #corgiplan_job{}) -> non_neg_integer().
% max_retries(#corgiplan_job{job_server_name = JobServerName}) ->
%     JobServerName:max_retries().

% -spec inception_time(Job :: #corgiplan_job{}) -> calendar:datetime1970().
% inception_time(#corgiplan_job{job_server_name = JobServerName}) ->
%     JobServerName:inception_time().

% this function is probably redundant
register(JobServerName) ->
    StartMFA =
        {corgiplan_job, start_link, [JobServerName, {global, corgiplan_job_state_manager}]},
    RegistrationOutcome =
        supervisor:start_child({global, corgiplan_sup},
                               #{id => JobServerName, start => StartMFA}),
    ok = handle_registration_outcome(RegistrationOutcome),
    ok.

run_parallel([]) ->
    [];
run_parallel([{Fun, Timeout} | Tail]) ->
    Pid = spawn_link(?MODULE, Fun, [self()]),
    TailResult = run_parallel(Tail),
    receive
        {Pid, ok, Result} ->
            [Result | TailResult]
    after Timeout ->
        exit(task_execution_timeout)
    end.

stop(Name) ->
    gen_server:call(Name, stop).

start_link(JobServerName, JobStateManagerServerName) ->
    ModuleName = corgiplan_job,
    Args = {JobServerName, JobStateManagerServerName},
    Options = [],
    gen_server:start_link({global, JobServerName}, ModuleName, Args, Options).

%% gen_server

-record(state, {job :: #corgiplan_job{}, job_state_manager :: term()}).

init({JobServerName, JobStateManagerServerName}) ->
    Job0 = make_init_job_state(JobServerName),
    {ok, Job1} = corgiplan_job_state_manager:init_job(JobStateManagerServerName, Job0),
    Job2 = decrement_attempt_count(Job1),
    Job3 = shift_to_next_exec_point(Job2, JobStateManagerServerName),
    {ok, Job4} =
        corgiplan_job_state_manager:arm_job_for_execution(JobStateManagerServerName, Job3),
    TimeGapUntilExecution0 =
        get_time_until_next_execution(Job4#corgiplan_job.armed_execution_time),
    TimeGapUntilExecution1 =
        max(JobServerName:cooldown_millis(), max(5000, TimeGapUntilExecution0)),
    {ok,
     #state{job = Job4, job_state_manager = JobStateManagerServerName},
     TimeGapUntilExecution1}.

handle_call(get_crontab_schedule,
            _From,
            State = #state{job = #corgiplan_job{job_server_name = JobServerName}}) ->
    Result = JobServerName:crontab_schedule(),
    {reply, {ok, Result}, State};
handle_call(get_max_retries,
            _From,
            State = #state{job = #corgiplan_job{job_server_name = JobServerName}}) ->
    Result = JobServerName:max_retries(),
    {reply, {ok, Result}, State};
handle_call(get_execution_result,
            _From,
            State = #state{job = #corgiplan_job{job_server_name = JobServerName}}) ->
    Result = JobServerName:inception_time(),
    {reply, {ok, Result}, State};
handle_call(stop, _From, State) ->
    {stop, normal, stopped, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timeout,
            State =
                #state{job =
                           #corgiplan_job{job_server_name = JobServerName,
                                          armed_execution_time = ExecutionTimeUTC},
                       job_state_manager = StateManager}) ->
    ok =
        JobServerName:execution_plan(ExecutionTimeUTC), % let it crash and be re-tried on the supervisor level.
    UpdatedJob = update_job_to_next_exec_point(State#state.job),
    ok = corgiplan_job_state_manager:mark_success(StateManager, UpdatedJob),
    TimeGapUntilExecution =
        get_time_until_next_execution(UpdatedJob#corgiplan_job.armed_execution_time),
    %%handle between fail and ok. The will but us into back with different timeouts.
    {noreply, State#state{job = UpdatedJob}, TimeGapUntilExecution};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% helper functions

% -spec get_next_execution_time(Job :: #corgiplan_job{}) -> calendar:datetime1970().
% get_next_execution_time(#corgiplan_job{job_server_name = JobServerName,
%                                        armed_execution_time = ArmedExecutionTime}) ->
%     ParsedCrontabSpec =
%         cron_ops:time_specs(
%             JobServerName:crontab_schedule()),
%     cron_ops:next(ArmedExecutionTime, ParsedCrontabSpec).

-spec handle_registration_outcome({error, Reason :: term()} |
                                  {ok, undefined | pid()} |
                                  {ok, undefined | pid(), Info :: term()}) ->
                                     ok | {error, Result :: term()}.
handle_registration_outcome({ok, undefined}) ->
    ok;
handle_registration_outcome({ok, _Child}) ->
    ok;
handle_registration_outcome({ok, _Child, _Info}) ->
    ok;
handle_registration_outcome({error, already_present}) ->
    ok;
handle_registration_outcome({error, {already_started, _Child}}) ->
    ok;
handle_registration_outcome({error, Reason}) ->
    {error, Reason}.

-spec get_time_until_next_execution(NextExecutionTimeUTC :: calendar:datetime1970()) ->
                                       integer().
get_time_until_next_execution(NextExecutionTimeUTC) ->
    NextGregorianSeconds = calendar:datetime_to_gregorian_seconds(NextExecutionTimeUTC),
    CurrentTime = calendar:local_time(),
    [CurrentTimeUTC] = calendar:local_time_to_universal_time_dst(CurrentTime),
    CurrentTimeUTCSeconds = calendar:datetime_to_gregorian_seconds(CurrentTimeUTC),
    SecondsDiff = max(0, NextGregorianSeconds - CurrentTimeUTCSeconds),
    SecondsDiff * 1000.

-spec decrement_attempt_count(Job :: #corgiplan_job{}) -> #corgiplan_job{}.
decrement_attempt_count(Job = #corgiplan_job{current_attempts_count =
                                                 CurrentAttemptsCount}) ->
    UpdatedAttemptsCount = CurrentAttemptsCount - 1,
    Job#corgiplan_job{current_attempts_count = UpdatedAttemptsCount}.

-spec shift_to_next_exec_point(Job :: #corgiplan_job{},
                               JobStateManagerServerName :: term()) ->
                                  #corgiplan_job{}.
shift_to_next_exec_point(Job = #corgiplan_job{current_attempts_count = 0},
                         JobStateManagerServerName) ->
    ok = corgiplan_job_state_manager:mark_max_retry_failure(JobStateManagerServerName, Job),
    update_job_to_next_exec_point(Job);
shift_to_next_exec_point(Job, _JobStateManagerServerName) ->
    Job.

update_job_to_next_exec_point(Job = #corgiplan_job{job_server_name = JobServerName}) ->
    Job#corgiplan_job{current_attempts_count = JobServerName:max_retries(),
                      armed_execution_time = get_next_execution_time(Job)}.
