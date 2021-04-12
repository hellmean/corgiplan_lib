-module(init_db).

-include("corgiplan_job.hrl").
-include("corgiplan_execution_result.hrl").

-export([init/0]).

init() ->
    mnesia:create_schema([node()]),
    mnesia:start(),
    mnesia:create_table(corgiplan_job, [{attributes, record_info(fields, corgiplan_job)}]),
    mnesia:create_table(corgiplan_execution_result,
                        [{attributes, record_info(fields, corgiplan_execution_result)},
                         {type, bag}]).
