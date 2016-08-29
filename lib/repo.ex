defmodule Eredisx.Repo do
  defmacro __using__(opts) do
    quote do
      use Supervisor
      import Eredisx.Client

      @default_eredis_config [
        host: 'localhost',
        port: 6379,
        database: 0,
        reconnect: :no_reconnect,
        max_queue: :infinity
      ]
      @default_poolboy_config [
        pool_size: 5,
        max_overflow: 10
      ]

      @otp_app unquote(Keyword.get(opts, :otp_app))

      def start_link do
        Supervisor.start_link(__MODULE__, [])
      end

      def init(_opts \\ []) do
        config = Application.get_env(@otp_app, __MODULE__)
        poolboy_option = parse_poolboy_options(config, @default_poolboy_config)
        eredis_option = parse_eredis_option(config, @default_eredis_config)

        children = [:poolboy.child_spec(__MODULE__, poolboy_option, eredis_option)]

        supervise(children, strategy: :one_for_one)
      end

      defp parse_poolboy_options(config, default_poolboy_config) do
        [
          {:name, {:local, __MODULE__}},
          {:worker_module, :eredis},
          {:size, Keyword.get(config, :pool_size) || Keyword.get(default_poolboy_config, :pool_size)},
          {:max_overflow, Keyword.get(config, :pool_max_overflow) || Keyword.get(default_poolboy_config, :max_overflow)}
        ]
      end

      defp parse_eredis_option(config, default_eredis_config) do
        to_chars = fn(v) -> if(is_binary(v), do: String.to_char_list(v), else: v) end
        Enum.map(default_eredis_config, fn({key, default_value}) ->
          {key, to_chars.(Keyword.get(config, key, default_value))}
        end)
      end

      defdelegate start_transaction, to: Eredisx.Client
      def end_transaction(use_pool \\ true) do
        if use_pool do
          :poolboy.transaction(__MODULE__, &(Eredisx.Client.end_transaction(pid: &1)))
        else
          execute_wo_pool(&(Eredisx.Client.end_transaction(pid: &1)))
        end
      end
      defdelegate start_pipeline, to: Eredisx.Client
      def exec_pipeline(use_pool \\ true) do
        if use_pool do
          :poolboy.transaction(__MODULE__, &(Eredisx.Client.exec_pipeline(pid: &1)))
        else
          execute_wo_pool(&(Eredisx.Client.exec_pipeline(pid: &1)))
        end
      end
      def query(api, args, use_pool \\ true) do
        if use_pool do
          :poolboy.transaction(__MODULE__, &(Eredisx.Client.query(api, args, pid: &1)))
        else
          execute_wo_pool(&(Eredisx.Client.query(api, args, pid: &1)))
        end
      end
      def query_pipeline(commands, use_pool \\ true) when is_list(commands) do
        if use_pool do
          :poolboy.transaction(__MODULE__, &(:eredis.qp(&1, commands)))
        else
          execute_wo_pool(&(:eredis.qp(&1, commands)))
        end
      end

      defp execute_wo_pool(executable) do
        config = Application.get_env(@otp_app, __MODULE__) 
                  |> parse_eredis_option(@default_eredis_config)
        client = :eredis.start_link(config) |> elem(1)
        result = executable.(client)
        :eredis.stop(client)
        result
      end

      @doc """
      Usage:
      ```
      AnyRepo.pipeline do
      key = "testvalue"
      Eredisx.Api.String.set(key, 100)
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.set(key, "hogefuga")
      end
      ```
      """
      defmacro pipeline(block) do
        quote do
          pipeline unquote(block), true
        end
      end
      @doc """
      Usage:
      ```
      Execute pipeline without connection pool
      AnyRepo.pipeline_wo_pool do
      key = "testvalue"
      Eredisx.Api.String.set(key, 100)
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.set(key, "hogefuga")
      end
      ```
      """
      defmacro pipeline_wo_pool(block) do
        quote do
          pipeline unquote(block), false
        end
      end

      defmacro pipeline(block, use_pool) do
        quote do
          res = Task.async(fn ->
            __MODULE__.start_pipeline
            unquote(Keyword.get(block, :do, nil))
            __MODULE__.exec_pipeline(unquote(use_pool))
          end)
          |> Task.await
          |> Enum.unzip
          {if(elem(res, 0) |> Enum.all?(&(&1 == :ok)), do: :ok, else: :error), elem(res, 1)}
        end
      end

      @doc """
      Usage:
      ```
      AnyRepo.transaction do
      key = "testvalue_transaction"
      Eredisx.Api.General.del(key)
      Eredisx.Api.String.set(key, "fugafuga")
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.set(key, "fugafuga" |> String.reverse)
      end
      ```
      """
      defmacro transaction(block) do
        quote do
          transaction unquote(block), true
        end
      end

      @doc """
      Execute transaction without connection pool

      Usage:
      ```
      AnyRepo.transaction_wo_pool do
      key = "testvalue_transaction"
      Eredisx.Api.General.del(key)
      Eredisx.Api.String.set(key, "fugafuga")
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.incr(key)
      Eredisx.Api.String.set(key, "fugafuga" |> String.reverse)
      end
      ```
      """
      defmacro transaction_wo_pool(block) do
        quote do
          transaction unquote(block), false
        end
      end

      defmacro transaction(block, use_pool) do
        quote do
          __MODULE__.start_transaction
          unquote(Keyword.get(block, :do, nil))
          {:ok, result} = __MODULE__.end_transaction(unquote(use_pool))

          errors = 
            case result do
              :undefined -> [:error]
              r -> r |> Enum.filter(fn(v) -> 
                is_binary(r) && Regex.match?(~r/\Aerr /i, v) 
                end)
            end
          {if(length(errors) > 0, do: :error, else: :ok), result}
        end
      end
    end

  end
end
