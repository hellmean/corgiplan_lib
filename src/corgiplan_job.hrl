-record(corgiplan_job,
        {job_server_name :: term(),
         armed_execution_time :: calendar:datetime1970(),
         current_attempts_count :: integer()}).
