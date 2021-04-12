-module(corgiplan_executor).

-behaviour(gen_server).

-callback init() -> {ok, State :: term()} | {stop, Reason :: term()}.
-callback handle_request(Action :: term(), Parameters :: term(), State :: term()) ->
                            {ok, Result :: term(), State :: term()} |
                            {fail, Reason :: term()} |
                            in_progress.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
%% API
-export([make_request/5, start_link/2, stop/1]).

-record(executor_state, {callback_module :: module(), state :: term()}).

make_request(ServerName, Token, Action, Parameters, Timeout) ->
    gen_server:call(ServerName, {request, Token, Action, Parameters}, Timeout).

start_link(ServerName, CallbackModule) ->
    gen_server:start_link(ServerName, corgiplan_executor, CallbackModule, []).

stop(ServerName) ->
    gen_server:call(ServerName, stop).

%% gen_server

init(CallbackModule) ->
    {ok, State} = CallbackModule:init(),
    {ok, #executor_state{callback_module = CallbackModule, state = State}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};
handle_call({request, Token, Action, Parameters},
            _From,
            #executor_state{state = State, callback_module = CallbackModule}) ->
    ok = corgitoken_validator:validate(Token, Action),
    {ok, Result, NewState} = CallbackModule:handle_request(Action, Parameters, State),
    {reply, Result, #executor_state{state = NewState, callback_module = CallbackModule}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
