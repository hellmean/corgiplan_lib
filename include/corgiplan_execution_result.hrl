-record(corgiplan_execution_result,
        {job_server_name :: term(),
         execution_time :: calendar:datetime1970(),
         status :: success | max_retry_failure | marked_as_failure}).
