-module(corgiplan_job).

-behaviour(gen_server).

-include("corgiplan_job.hrl").

%% callbacks

-callback inception_time() -> calendar:datetime1970().
-callback crontab_schedule() -> term().
%if we return fail this stops execution without retries.
-callback max_retries() -> non_neg_integer().
-callback execution_plan(ExecutionTimeUTC :: calendar:datetime1970()) -> ok | fail.

-export([register/1, stop/1, start_link/1, run_parallel/1, max_retries/1,
         inception_time/1, make_init_job_state/1, get_next_execution_time/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% API

-spec get_next_execution_time(Job :: #corgiplan_job{}) -> calendar:datetime1970().
get_next_execution_time(#corgiplan_job{job_server_name = JobServerName,
                                       armed_execution_time = CurrentExecutionTimeUTC}) ->
    ParsedCrontabSpec =
        cron_ops:time_specs(
            JobServerName:crontab_schedule()),
    cron_ops:next(CurrentExecutionTimeUTC, ParsedCrontabSpec).

-spec make_init_job_state(JobServerName :: term()) -> #corgiplan_job{}.
make_init_job_state(JobServerName) ->
    JobAtInception =
        #corgiplan_job{job_server_name = JobServerName,
                       armed_execution_time = JobServerName:inception_time(),
                       current_attempts_count = JobServerName:max_retries()},
    NextExecutionTimeUTC = get_next_execution_time(JobAtInception),
    JobAtInception#corgiplan_job{armed_execution_time = NextExecutionTimeUTC}.

-spec max_retries(Job :: #corgiplan_job{}) -> non_neg_integer().
max_retries(#corgiplan_job{job_server_name = JobServerName}) ->
    JobServerName:max_retries().

-spec inception_time(Job :: #corgiplan_job{}) -> calendar:datetime1970().
inception_time(#corgiplan_job{job_server_name = JobServerName}) ->
    JobServerName:inception_time().

register(JobServerName) ->
    StartMFA = {corgiplan_job, start_link, [JobServerName]},
    RegistrationOutcome =
        supervisor:start_child({global, corgiplan_sup},
                               #{id => JobServerName, start => StartMFA}),
    %also i think we need to start a separate supervisor for the job can connect that to the global one
    %maybe we can do a separate server per run?
    %how are we going to block all the others?
    %and then release them?
    %it would be possible through a chain of processes i guess
    %it looks like keeping count of attempts in mnesia is the easiest approach
    ok = handle_registration_outcome(RegistrationOutcome),
    ok.    %ChildSpecs=..., handle_registertration_outcome(supervisor:start_child(SupRef, ChildSpecs)).

%the child specs should descripe it as transient process
%_sup:start_child(Name).
%idea:
%SchedulerSupervisor should enumerate all of its children every minute or so and generate a new sequence of execution
%the sequence should be formed by looking up Mnesia for the last unfinished execution time using child_id() as the key
%this can be done with supervisor:which_children/1
%also once this list is reformed we check if it is time to run the Head. If yes we do
%gen_server:cast(Head, execute). Looks good so far.
%when the execution is finished be shutdown normally.
%the job gen server then should be able to handle the retries -- no
%if the this gen_server fails the Scheduler supervisor should handle the restart and add it to the top of the
%schedule, by again querying Mnesia for the last unfinished execution time.
%Mnesia should also keep the retries count, if it crosses the threshold we bump the execution time with cron interval

% another design question: how to run parrall tasks

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

start_link(JobServerName) ->
    ModuleName = corgiplan_job,
    Args = JobServerName,
    Options = [],
    gen_server:start_link({global, JobServerName}, ModuleName, Args, Options).

%% gen_server

init(JobServerName) ->
    {ok, Job} = corgiplan_job_state_manager:arm_job_for_execution(JobServerName),
    TimeGapUntilExecution =
        get_time_until_next_execution(Job#corgiplan_job.armed_execution_time),
    {ok, Job, TimeGapUntilExecution}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

% handle_call(execute, _From, State=#state{callback_module=CallbackModule}) ->
%     CallbackModule:execution_plan(State),
%     {reply, ok, State}.
% probably need to create a reusable function and call it both here in and in handle_info

handle_info(timeout,
            #corgiplan_job{job_server_name = JobServerName,
                           armed_execution_time = ExecutionTimeUTC}) ->
    ok =
        JobServerName:execution_plan(ExecutionTimeUTC), % let it crash and be re-tried on the supervisor level.
    {ok, UpdatedJob} =
        corgiplan_job_state_manager:mark_success(JobServerName, ExecutionTimeUTC),
    TimeGapUntilExecution =
        get_time_until_next_execution(UpdatedJob#corgiplan_job.armed_execution_time),
    %%handle between fail and ok. The will but us into back with different timeouts.
    {noreply, UpdatedJob, TimeGapUntilExecution};
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
