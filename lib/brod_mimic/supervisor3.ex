defmodule BrodMimic.Supervisor3 do
  @moduledoc """
  This is a general process supervisor built upon gen_server.erl

  ## Notes from brod_supervisor3.erl

  ```
  Copied from https://github.com/klarna/supervisor3
  This file is a copy of supervisor.erl from the R16B Erlang/OTP
  distribution, with the following modifications:

  1) the module name is supervisor2

  2) a find_child/2 utility function has been added

  3) Added an 'intrinsic' restart type. Like the transient type, this
     type means the child should only be restarted if the child exits
     abnormally. Unlike the transient type, if the child exits
     normally, the supervisor itself also exits normally. If the
     child is a supervisor and it exits normally (i.e. with reason of
     'shutdown') then the child's parent also exits normally.

  4) child specifications can contain, as the restart type, a tuple
     {permanent, Delay} | {transient, Delay} | {intrinsic, Delay}
     where Delay >= 0 (see point (4) below for intrinsic). The delay,
     in seconds, indicates what should happen if a child, upon being
     restarted, exceeds the MaxT and MaxR parameters. Thus, if a
     child exits, it is restarted as normal. If it exits sufficiently
     quickly and often to exceed the boundaries set by the MaxT and
     MaxR parameters, and a Delay is specified, then rather than
     stopping the supervisor, the supervisor instead continues and
     tries to start up the child again, Delay seconds later.

    Note that if a child is delay-restarted this will reset the
    count of restarts towrds MaxR and MaxT. This matters if MaxT >
    Delay, since otherwise we would fail to restart after the delay.

    Sometimes, you may wish for a transient or intrinsic child to
    exit abnormally so that it gets restarted, but still log
    nothing. gen_server will log any exit reason other than
    'normal', 'shutdown' or {'shutdown', _}. Thus the exit reason of
    {'shutdown', 'restart'} is interpreted to mean you wish the
    child to be restarted according to the delay parameters, but
    gen_server will not log the error. Thus from gen_server's
    perspective it's a normal exit, whilst from supervisor's
    perspective, it's an abnormal exit.

    5) normal, and {shutdown, _} exit reasons are all treated the same
       (i.e. are regarded as normal exits)

    6) rename the module to supervisor3

    7) introduce post_init callback

    8) call os:timestamp/0 and timer:now_diff/2 for timestamps

    9) ignore delayed retry in MaxR accumulation

    Modifications 1-5 are (C) 2010-2013 GoPivotal, Inc.
    Modifications 6-9 are (C) 2015 Klarna AB

    %CopyrightBegin%

    Copyright Ericsson AB 1996-2012. All Rights Reserved.

    The contents of this file are subject to the Erlang Public License,
    Version 1.1, (the "License"); you may not use this file except in
    compliance with the License. You should have received a copy of the
    Erlang Public License along with this software. If not, it can be
    retrieved online at http://www.erlang.org/.

    Software distributed under the License is distributed on an "AS IS"
    basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
    the License for the specific language governing rights and limitations
    under the License.

    %CopyrightEnd%
  ```
  """
  use GenServer

  import Record, only: [defrecordp: 2]

  defrecordp(:child,
    pid: :undefined,
    name: :undefined,
    mfargs: :undefined,
    restart_type: :undefined,
    shutdown: :undefined,
    child_type: :undefined,
    modules: []
  )

  defrecordp(:state,
    name: :undefined,
    strategy: :undefined,
    children: [],
    dynamics: :undefined,
    intensity: :undefined,
    period: :undefined,
    restarts: [],
    module: :undefined,
    args: :undefined
  )

  @type child() :: :undefined | pid()
  @type child_id() :: term()
  @type mfargs() :: {module(), atom(), [term()] | :undefined}
  @type modules() :: [module()] | :dynamic
  @type delay() :: non_neg_integer()
  @type restart() ::
          :permanent
          | :transient
          | :temporary
          | :intrinsic
          | {:permanent, delay()}
          | {:transient, delay()}
          | {:intrinsic, delay()}
  @type shutdown() :: :brutal_kill | timeout()
  @type worker() :: :worker | :supervisor
  @type sup_name() :: {:local, name :: atom()} | {:global, name :: atom()}
  @type sup_ref() ::
          (name :: atom())
          | {name :: atom(), node :: node()}
          | {:global, name :: atom()}
          | pid()
  @type child_spec() ::
          {id :: child_id(), start_func :: mfargs(), restart :: restart(), shutdown :: shutdown(), type :: worker(),
           modules :: modules()}

  @type strategy() :: :one_for_all | :one_for_one | :rest_for_one | :simple_one_for_one

  @type tref() :: reference()
  @type child_record_pid() ::
          :undefined | child() | {:restarting, pid()} | {:delayed_restart, tref()} | [pid()]
  @type dynamics() :: :dict.dict() | :sets.set()

  @type child_rec() ::
          record(:child,
            pid: child_record_pid(),
            name: child_id(),
            mfargs: mfargs(),
            restart_type: restart(),
            shutdown: shutdown(),
            child_type: worker(),
            modules: modules()
          )

  @type state() ::
          record(:state,
            name: atom(),
            strategy: strategy(),
            children: [child_rec()],
            dynamics: dynamics(),
            intensity: non_neg_integer(),
            period: pos_integer(),
            restarts: list(),
            module: module(),
            args: list()
          )

  @type startlink_err() :: {:already_started, pid()} | {:shutdown, term()} | term()
  @type startlink_ret() :: {:ok, pid()} | :ignore | {:error, startlink_err()}
  @type startchild_err() :: :already_present | {:already_started, child()} | term()
  @type startchild_ret() :: {:ok, child()} | {:ok, child(), term()} | {:error, startchild_err()}
  @type restart_child_err() ::
          {:error, :running | :restarting | :not_found | :simple_one_for_one | term()}

  @callback init(args :: term()) ::
              {
                :ok,
                {
                  {
                    restart_strategy :: strategy(),
                    max_r :: non_neg_integer(),
                    max_t :: non_neg_integer()
                  },
                  [child_spec :: child_spec()]
                }
              }
              | :ignore
              | :post_init

  @callback post_init(args :: term()) ::
              {
                :ok,
                {
                  {
                    restart_strategy :: strategy(),
                    max_r :: non_neg_integer(),
                    max_t :: non_neg_integer()
                  },
                  [child_spec :: child_spec()]
                }
              }
              | :ignore

  @optional_callbacks [post_init: 1]

  @spec start_link(module(), term()) :: startlink_ret()
  def start_link(mod, args) do
    GenServer.start_link(BrodMimic.Supervisor3, {:self, mod, args}, [])
  end

  @spec start_link(sup_name(), module(), term()) :: startlink_ret()
  def start_link(sup_name, mod, args) do
    :gen_server.start_link(sup_name, BrodMimic.Supervisor3, {sup_name, mod, args}, [])
  end

  @spec start_child(sup_ref(), child_spec() | [term()]) :: startchild_ret()
  def start_child(supervisor, child_spec) do
    call(supervisor, {:start_child, child_spec})
  end

  @spec restart_child(sup_ref(), child_id()) :: {:ok, child()} | {:ok, child(), term()} | restart_child_err()
  def restart_child(supervisor, name) do
    call(supervisor, {:restart_child, name})
  end

  @spec delete_child(sup_ref(), child_id()) :: :ok | {:error, any()}
  def delete_child(supervisor, name) do
    call(supervisor, {:delete_child, name})
  end

  def terminate_child(supervisor, name) do
    call(supervisor, {:terminate_child, name})
  end

  def which_children(supervisor) do
    call(supervisor, :which_children)
  end

  def count_children(supervisor) do
    call(supervisor, :count_children)
  end

  def find_child(supervisor, name) do
    supervisor
    |> which_children()
    |> Enum.find_value([], fn {name1, pid, _, _} ->
      if name == name1 do
        [pid]
      end
    end)
  end

  defp call(supervisor, req) do
    GenServer.call(supervisor, req, :infinity)
  end

  def check_childspecs(child_specs) when is_list(child_specs) do
    case check_startspec(child_specs) do
      {:ok, _} ->
        :ok

      error ->
        {:error, error}
    end
  end

  def check_childspecs(x) do
    {:error, {:badarg, x}}
  end

  def try_again_restart(supervisor, child, reason) do
    cast(supervisor, {:try_again_restart, child, reason})
  end

  defp cast(supervisor, req) do
    GenServer.cast(supervisor, req)
  end

  def init({sup_name, mod, args}) do
    Process.flag(:trap_exit, true)

    case mod.init(args) do
      {:ok, {sup_flags, start_spec}} ->
        do_init(sup_name, sup_flags, start_spec, mod, args)

      :post_init ->
        send(self(), {:post_init, sup_name, mod, args})
        {:ok, state()}

      :ignore ->
        :ignore

      error ->
        {:stop, {:bad_return, {mod, :init, error}}}
    end
  end

  defp init_children(state, start_spec) do
    sup_name = state(state, :name)

    case check_startspec(start_spec) do
      {:ok, children} ->
        case start_children(children, sup_name) do
          {:ok, n_children} ->
            {:ok, state(state, children: n_children)}

          {:error, n_children, reason} ->
            terminate_children(n_children, sup_name)
            {:stop, {:shutdown, reason}}
        end

      error ->
        {:stop, {:start_spec, error}}
    end
  end

  defp init_dynamic(state, [start_spec]) do
    case check_startspec([start_spec]) do
      {:ok, children} ->
        {:ok, state(state, children: children)}

      error ->
        {:stop, {:start_spec, error}}
    end
  end

  defp init_dynamic(_state, start_spec) do
    {:stop, {:bad_start_spec, start_spec}}
  end

  defp start_children(children, sup_name) do
    start_children(children, [], sup_name)
  end

  defp start_children([child | chs], n_children, sup_name) do
    case do_start_child(sup_name, child) do
      {:ok, :undefined}
      when child(child, :restart_type) === :temporary ->
        start_children(chs, n_children, sup_name)

      {:ok, pid} ->
        start_children(chs, [child(child, pid: pid) | n_children], sup_name)

      {:ok, pid, _extra} ->
        start_children(chs, [child(child, pid: pid) | n_children], sup_name)

      {:error, reason} ->
        report_error(:start_error, reason, child, sup_name)

        {:error, :lists.reverse(chs) ++ [child | n_children], {:failed_to_start_child, child(child, :name), reason}}
    end
  end

  defp start_children([], n_children, _sup_name) do
    {:ok, n_children}
  end

  defp do_start_child(sup_name, child) do
    child(mfargs: {m, f, args}) = child

    case (try do
            apply(m, f, args)
          catch
            :error, e -> {:EXIT, {e, __STACKTRACE__}}
            :exit, e -> {:EXIT, e}
            e -> e
          end) do
      {:ok, pid} when is_pid(pid) ->
        n_child = child(child, pid: pid)
        report_progress(n_child, sup_name)
        {:ok, pid}

      {:ok, pid, extra} when is_pid(pid) ->
        n_child = child(child, pid: pid)
        report_progress(n_child, sup_name)
        {:ok, pid, extra}

      :ignore ->
        {:ok, :undefined}

      {:error, what} ->
        {:error, what}

      what ->
        {:error, what}
    end
  end

  defp do_start_child_i(m, f, a) do
    case (try do
            apply(m, f, a)
          catch
            :error, e -> {:EXIT, {e, __STACKTRACE__}}
            :exit, e -> {:EXIT, e}
            e -> e
          end) do
      {:ok, pid} when is_pid(pid) ->
        {:ok, pid}

      {:ok, pid, extra} when is_pid(pid) ->
        {:ok, pid, extra}

      :ignore ->
        {:ok, :undefined}

      {:error, error} ->
        {:error, error}

      what ->
        {:error, what}
    end
  end

  def handle_call({:start_child, e_args}, _from, state)
      when state(state, :strategy) === :simple_one_for_one do
    child = hd(state(state, :children))
    child(mfargs: {m, f, a}) = child
    args = a ++ e_args

    case do_start_child_i(m, f, args) do
      {:ok, :undefined}
      when child(child, :restart_type) === :temporary ->
        {:reply, {:ok, :undefined}, state}

      {:ok, pid} ->
        n_state = save_dynamic_child(child(child, :restart_type), pid, args, state)
        {:reply, {:ok, pid}, n_state}

      {:ok, pid, extra} ->
        n_state = save_dynamic_child(child(child, :restart_type), pid, args, state)
        {:reply, {:ok, pid, extra}, n_state}

      what ->
        {:reply, what, state}
    end
  end

  def handle_call({:terminate_child, name}, _from, state)
      when not is_pid(name) and
             state(state, :strategy) === :simple_one_for_one do
    {:reply, {:error, :simple_one_for_one}, state}
  end

  def handle_call({:terminate_child, name}, _from, state) do
    case get_child(name, state, state(state, :strategy) === :simple_one_for_one) do
      {:value, child} ->
        case do_terminate(child, state(state, :name)) do
          child(restart_type: rt)
          when rt === :temporary or
                 state(state, :strategy) === :simple_one_for_one ->
            {:reply, :ok, state_del_child(child, state)}

          n_child ->
            {:reply, :ok, replace_child(n_child, state)}
        end

      false ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({_req, _data}, _from, state)
      when state(state, :strategy) === :simple_one_for_one do
    {:reply, {:error, :simple_one_for_one}, state}
  end

  def handle_call({:start_child, child_spec}, _from, state) do
    case check_childspec(child_spec) do
      {:ok, child} ->
        {resp, n_state} = handle_start_child(child, state)
        {:reply, resp, n_state}

      what ->
        {:reply, {:error, what}, state}
    end
  end

  def handle_call({:restart_child, name}, _from, state) do
    case get_child(name, state) do
      {:value, child} when child(child, :pid) === :undefined ->
        case do_start_child(state(state, :name), child) do
          {:ok, pid} ->
            n_state = replace_child(child(child, pid: pid), state)
            {:reply, {:ok, pid}, n_state}

          {:ok, pid, extra} ->
            n_state = replace_child(child(child, pid: pid), state)
            {:reply, {:ok, pid, extra}, n_state}

          error ->
            {:reply, error, state}
        end

      {:value, child(pid: {:restarting, _})} ->
        {:reply, {:error, :restarting}, state}

      {:value, child(pid: {:delayed_restart, _})} ->
        {:reply, {:error, :restarting}, state}

      {:value, _} ->
        {:reply, {:error, :running}, state}

      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:delete_child, name}, _from, state) do
    case get_child(name, state) do
      {:value, child} when child(child, :pid) === :undefined ->
        n_state = remove_child(child, state)
        {:reply, :ok, n_state}

      {:value, child(pid: {:restarting, _})} ->
        {:reply, {:error, :restarting}, state}

      {:value, child(pid: {:delayed_restart, _})} ->
        {:reply, {:error, :restarting}, state}

      {:value, _} ->
        {:reply, {:error, :running}, state}

      _ ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(
        :which_children,
        _from,
        state(children: [child(restart_type: :temporary, child_type: ct, modules: mods)]) = state
      )
      when state(state, :strategy) === :simple_one_for_one do
    reply =
      :lists.map(
        fn pid ->
          {:undefined, pid, ct, mods}
        end,
        :sets.to_list(
          dynamics_db(
            :temporary,
            state(state, :dynamics)
          )
        )
      )

    {:reply, reply, state}
  end

  def handle_call(
        :which_children,
        _from,
        state(children: [child(restart_type: r_type, child_type: ct, modules: mods)]) = state
      )
      when state(state, :strategy) === :simple_one_for_one do
    reply =
      :lists.map(
        fn
          {{:restarting, _}, _} ->
            {:undefined, :restarting, ct, mods}

          {pid, _} ->
            {:undefined, pid, ct, mods}
        end,
        :dict.to_list(dynamics_db(r_type, state(state, :dynamics)))
      )

    {:reply, reply, state}
  end

  def handle_call(:which_children, _from, state) do
    resp =
      :lists.map(
        fn
          child(pid: {:restarting, _}, name: name, child_type: child_type, modules: mods) ->
            {name, :restarting, child_type, mods}

          child(pid: {:delayed_restart, _}, name: name, child_type: child_type, modules: mods) ->
            {name, :restarting, child_type, mods}

          child(pid: pid, name: name, child_type: child_type, modules: mods) ->
            {name, pid, child_type, mods}
        end,
        state(state, :children)
      )

    {:reply, resp, state}
  end

  def handle_call(
        :count_children,
        _from,
        state(
          children: [
            child(
              restart_type: :temporary,
              child_type: ct
            )
          ]
        ) = state
      )
      when state(state, :strategy) === :simple_one_for_one do
    {active, count} =
      :sets.fold(
        fn pid, {alive, tot} ->
          count_if_alive(pid, alive, tot)
        end,
        {0, 0},
        dynamics_db(:temporary, state(state, :dynamics))
      )

    reply = child_type(ct, active, count)
    {:reply, reply, state}
  end

  def handle_call(
        :count_children,
        _from,
        state(
          children: [
            child(
              restart_type: r_type,
              child_type: ct
            )
          ]
        ) = state
      )
      when state(state, :strategy) === :simple_one_for_one do
    {active, count} =
      :dict.fold(
        fn pid, _val, {alive, tot} ->
          count_if_alive(pid, alive, tot)
        end,
        {0, 0},
        dynamics_db(r_type, state(state, :dynamics))
      )

    reply = child_type(ct, active, count)
    {:reply, reply, state}
  end

  def handle_call(:count_children, _from, state) do
    {specs, active, supers, workers} =
      :lists.foldl(
        fn child, counts ->
          count_child(child, counts)
        end,
        {0, 0, 0, 0},
        state(state, :children)
      )

    reply = [{:specs, specs}, {:active, active}, {:supervisors, supers}, {:workers, workers}]
    {:reply, reply, state}
  end

  defp count_if_alive(pid, alive, total) do
    case is_pid(pid) and Process.alive?(pid) do
      true ->
        {alive + 1, total + 1}

      false ->
        {alive, total + 1}
    end
  end

  defp child_type(:supervisor, active, count) do
    [{:specs, 1}, {:active, active}, {:supervisors, count}, {:workers, 0}]
  end

  defp child_type(:worker, active, count) do
    [{:specs, 1}, {:active, active}, {:supervisors, 0}, {:workers, count}]
  end

  defp count_child(
         child(pid: pid, child_type: :worker),
         {specs, active, supers, workers}
       ) do
    case is_pid(pid) and Process.alive?(pid) do
      true ->
        {specs + 1, active + 1, supers, workers + 1}

      false ->
        {specs + 1, active, supers, workers + 1}
    end
  end

  defp count_child(
         child(pid: pid, child_type: :supervisor),
         {specs, active, supers, workers}
       ) do
    case is_pid(pid) and Process.alive?(pid) do
      true ->
        {specs + 1, active + 1, supers + 1, workers}

      false ->
        {specs + 1, active, supers + 1, workers}
    end
  end

  def handle_cast(
        {:try_again_restart, pid, reason},
        state(children: [child]) = state
      )
      when state(state, :strategy) === :simple_one_for_one do
    rt = child(child, :restart_type)
    r_pid = restarting(pid)

    case dynamic_child_args(r_pid, dynamics_db(rt, state(state, :dynamics))) do
      {:ok, args} ->
        {m, f, _} = child(child, :mfargs)
        n_child = child(child, pid: r_pid, mfargs: {m, f, args})
        try_restart(child(child, :restart_type), reason, n_child, state)

      :error ->
        {:noreply, state}
    end
  end

  def handle_cast({:try_again_restart, name, reason}, state) do
    case :lists.keysearch(name, child(:name), state(state, :children)) do
      {:value,
       child =
           child(
             pid: {:restarting, _},
             restart_type: restart_type
           )} ->
        try_restart(restart_type, reason, child, state)

      {:value,
       child =
           child(
             pid: {:delayed_restart, _},
             restart_type: restart_type
           )} ->
        try_restart(restart_type, reason, child, state)

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, reason}, state) do
    case restart_child(pid, reason, state) do
      {:ok, state1} ->
        {:noreply, state1}

      {:shutdown, state1} ->
        {:stop, :shutdown, state1}
    end
  end

  def handle_info(
        {:delayed_restart, {restart_type, _reason, child}},
        state
      )
      when state(state, :strategy) === :simple_one_for_one do
    reason = {BrodSupervisor3, :delayed_restart}
    try_restart(restart_type, reason, child, state(state, restarts: []))
  end

  def handle_info(
        {:delayed_restart, {restart_type, _reason, child}},
        state
      ) do
    reason = {BrodSupervisor3, :delayed_restart}

    case get_child(child(child, :name), state) do
      {:value, child1} ->
        try_restart(restart_type, reason, child1, state(state, restarts: []))

      _what ->
        {:noreply, state}
    end
  end

  def handle_info({:post_init, sup_name, mod, args}, state0) do
    res =
      case mod.post_init(args) do
        {:ok, {sup_flags, start_spec}} ->
          do_init(sup_name, sup_flags, start_spec, mod, args)

        error ->
          {:stop, {:bad_return, {mod, :post_init, error}}}
      end

    case res do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:stop, reason} ->
        {:stop, reason, state0}
    end
  end

  def handle_info(msg, state) do
    :error_logger.error_msg('Supervisor received unexpected message: ~p~n', [msg])
    {:noreply, state}
  end

  def terminate(_reason, state(children: [child]) = state)
      when state(state, :strategy) === :simple_one_for_one do
    terminate_dynamic_children(
      child,
      dynamics_db(
        child(child, :restart_type),
        state(state, :dynamics)
      ),
      state(state, :name)
    )
  end

  def terminate(_reason, state) do
    terminate_children(state(state, :children), state(state, :name))
  end

  def code_change(_, state, _) do
    case state(state, :module).init(state(state, :args)) do
      {:ok, {sup_flags, start_spec}} ->
        case (try do
                check_flags(sup_flags)
              catch
                :error, e -> {:EXIT, {e, __STACKTRACE__}}
                :exit, e -> {:EXIT, e}
                e -> e
              end) do
          :ok ->
            {strategy, max_intensity, period} = sup_flags

            update_childspec(
              state(state,
                strategy: strategy,
                intensity: max_intensity,
                period: period
              ),
              start_spec
            )

          error ->
            {:error, error}
        end

      :ignore ->
        {:ok, state}

      error ->
        error
    end
  end

  def format_status(:terminate, [_PDict, state]) do
    state
  end

  def format_status(_, [_PDict, state]) do
    [{:data, [{'State', state}]}, {:supervisor, [{'Callback', state(state, :module)}]}]
  end

  defp check_flags({strategy, max_intensity, period}) do
    valid_strategy(strategy)
    valid_intensity(max_intensity)
    valid_period(period)
    :ok
  end

  defp check_flags(what) do
    {:bad_flags, what}
  end

  defp update_childspec(state, start_spec)
       when state(state, :strategy) === :simple_one_for_one do
    case check_startspec(start_spec) do
      {:ok, [child]} ->
        {:ok, state(state, children: [child])}

      error ->
        {:error, error}
    end
  end

  defp update_childspec(state, start_spec) do
    case check_startspec(start_spec) do
      {:ok, children} ->
        old_c = state(state, :children)
        new_c = update_childspec1(old_c, children, [])
        {:ok, state(state, children: new_c)}

      error ->
        {:error, error}
    end
  end

  defp update_childspec1([child | old_c], children, keepOld) do
    case update_chsp(child, children) do
      {:ok, new_children} ->
        update_childspec1(old_c, new_children, keepOld)

      false ->
        update_childspec1(old_c, children, [child | keepOld])
    end
  end

  defp update_childspec1([], children, keepOld) do
    :lists.reverse(children ++ keepOld)
  end

  defp update_chsp(old_ch, children) do
    case :lists.map(
           fn
             ch
             when child(old_ch, :name) === child(ch, :name) ->
               child(ch, pid: child(old_ch, :pid))

             ch ->
               ch
           end,
           children
         ) do
      ^children ->
        false

      new_c ->
        {:ok, new_c}
    end
  end

  defp handle_start_child(child, state) do
    case get_child(child(child, :name), state) do
      false ->
        case do_start_child(state(state, :name), child) do
          {:ok, :undefined}
          when child(child, :restart_type) === :temporary ->
            {{:ok, :undefined}, state}

          {:ok, pid} ->
            {{:ok, pid}, save_child(child(child, pid: pid), state)}

          {:ok, pid, extra} ->
            {{:ok, pid, extra}, save_child(child(child, pid: pid), state)}

          {:error, what} ->
            {{:error, {what, child}}, state}
        end

      {:value, old_child} when is_pid(child(old_child, :pid)) ->
        {{:error, {:already_started, child(old_child, :pid)}}, state}

      {:value, _old_child} ->
        {{:error, :already_present}, state}
    end
  end

  defp restart_child(pid, reason, state(children: [child]) = state)
       when state(state, :strategy) === :simple_one_for_one do
    restart_type = child(child, :restart_type)

    case dynamic_child_args(
           pid,
           dynamics_db(
             restart_type,
             state(state, :dynamics)
           )
         ) do
      {:ok, args} ->
        {m, f, _} = child(child, :mfargs)
        n_child = child(child, pid: pid, mfargs: {m, f, args})
        do_restart(restart_type, reason, n_child, state)

      :error ->
        {:ok, state}
    end
  end

  defp restart_child(pid, reason, state) do
    children = state(state, :children)

    case :lists.keysearch(pid, child(:pid), children) do
      {:value, child(restart_type: restart_type) = child} ->
        do_restart(restart_type, reason, child, state)

      false ->
        {:ok, state}
    end
  end

  defp try_restart(restart_type, reason, child, state) do
    case handle_restart(restart_type, reason, child, state) do
      {:ok, n_state} ->
        {:noreply, n_state}

      {:shutdown, state2} ->
        {:stop, :shutdown, state2}
    end
  end

  defp do_restart(restart_type, reason, child, state) do
    maybe_report_error(restart_type, reason, child, state)
    handle_restart(restart_type, reason, child, state)
  end

  defp maybe_report_error(:permanent, reason, child, state) do
    report_child_termination(reason, child, state)
  end

  defp maybe_report_error({:permanent, _}, reason, child, state) do
    report_child_termination(reason, child, state)
  end

  defp maybe_report_error(_type, reason, child, state) do
    case is_abnormal_termination(reason) do
      true ->
        report_child_termination(reason, child, state)

      false ->
        :ok
    end
  end

  defp report_child_termination(reason, child, state) do
    report_error(:child_terminated, reason, child, state(state, :name))
  end

  defp handle_restart(:permanent, _reason, child, state) do
    restart(child, state)
  end

  defp handle_restart(:transient, reason, child, state) do
    restart_if_explicit_or_abnormal(
      &restart/2,
      &delete_child_and_continue/2,
      reason,
      child,
      state
    )
  end

  defp handle_restart(:intrinsic, reason, child, state) do
    restart_if_explicit_or_abnormal(&restart/2, &delete_child_and_stop/2, reason, child, state)
  end

  defp handle_restart(:temporary, _reason, child, state) do
    delete_child_and_continue(child, state)
  end

  defp handle_restart({:permanent, _delay} = restart, reason, child, state) do
    do_restart_delay(restart, reason, child, state)
  end

  defp handle_restart({:transient, _delay} = restart, reason, child, state) do
    restart_if_explicit_or_abnormal(
      defer_to_restart_delay(
        restart,
        reason
      ),
      &delete_child_and_continue/2,
      reason,
      child,
      state
    )
  end

  defp handle_restart({:intrinsic, _delay} = restart, reason, child, state) do
    restart_if_explicit_or_abnormal(
      defer_to_restart_delay(
        restart,
        reason
      ),
      &delete_child_and_stop/2,
      reason,
      child,
      state
    )
  end

  defp restart_if_explicit_or_abnormal(restartHow, otherwise, reason, child, state) do
    case reason == {:shutdown, :restart} or is_abnormal_termination(reason) do
      true ->
        restartHow.(child, state)

      false ->
        otherwise.(child, state)
    end
  end

  defp defer_to_restart_delay(restart, reason) do
    fn child, state ->
      do_restart_delay(restart, reason, child, state)
    end
  end

  defp delete_child_and_continue(child, state) do
    {:ok, state_del_child(child, state)}
  end

  defp delete_child_and_stop(child, state) do
    {:shutdown, state_del_child(child, state)}
  end

  defp is_abnormal_termination(:normal) do
    false
  end

  defp is_abnormal_termination(:shutdown) do
    false
  end

  defp is_abnormal_termination({:shutdown, _}) do
    false
  end

  defp is_abnormal_termination(_Other) do
    true
  end

  defp do_restart_delay({restart_type, delay}, reason, child, state) do
    is_clean_retry = reason === {BrodSupervisor3, :delayed_restart}

    case add_restart(state, is_clean_retry) do
      {:ok, n_state} ->
        maybe_restart(state(n_state, :strategy), child, n_state)

      {:terminate, _n_state} ->
        t_ref =
          Process.send_after(
            self(),
            {:delayed_restart, {{restart_type, delay}, reason, child}},
            trunc(delay * 1000)
          )

        n_state =
          case state(state, :strategy) === :simple_one_for_one do
            true ->
              state_del_child(child, state)

            false ->
              replace_child(
                child(child, pid: {:delayed_restart, t_ref}),
                state
              )
          end

        {:ok, n_state}
    end
  end

  defp restart(child, state) do
    case add_restart(state) do
      {:ok, n_state} ->
        maybe_restart(state(n_state, :strategy), child, n_state)

      {:terminate, n_state} ->
        report_error(:shutdown, :reached_max_restart_intensity, child, state(state, :name))
        {:shutdown, remove_child(child, n_state)}
    end
  end

  defp maybe_restart(strategy, child, state) do
    case restart(strategy, child, state) do
      {:try_again, reason, n_state2} ->
        id =
          case state(state, :strategy) === :simple_one_for_one do
            true -> child(child, :pid)
            false -> child(child, :name)
          end

        :timer.apply_after(0, BrodSupervisor3, :try_again_restart, [self(), id, reason])
        {:ok, n_state2}

      other ->
        other
    end
  end

  defp restart(:simple_one_for_one, child, state) do
    child(pid: old_pid, mfargs: {m, f, a}) = child

    dynamics =
      :dict.erase(
        old_pid,
        dynamics_db(
          child(child, :restart_type),
          state(state, :dynamics)
        )
      )

    case do_start_child_i(m, f, a) do
      {:ok, pid} ->
        n_state = state(state, dynamics: :dict.store(pid, a, dynamics))
        {:ok, n_state}

      {:ok, pid, _extra} ->
        n_state = state(state, dynamics: :dict.store(pid, a, dynamics))
        {:ok, n_state}

      {:error, error} ->
        n_state = state(state, dynamics: :dict.store(restarting(old_pid), a, dynamics))
        report_error(:start_error, error, child, state(state, :name))
        {:try_again, error, n_state}
    end
  end

  defp restart(:one_for_one, child, state) do
    old_pid = child(child, :pid)

    case do_start_child(state(state, :name), child) do
      {:ok, pid} ->
        n_state = replace_child(child(child, pid: pid), state)
        {:ok, n_state}

      {:ok, pid, _extra} ->
        n_state = replace_child(child(child, pid: pid), state)
        {:ok, n_state}

      {:error, reason} ->
        n_state =
          replace_child(
            child(child, pid: restarting(old_pid)),
            state
          )

        report_error(:start_error, reason, child, state(state, :name))
        {:try_again, reason, n_state}
    end
  end

  defp restart(:rest_for_one, child, state) do
    {ch_after, ch_before} =
      split_child(
        child(child, :pid),
        state(state, :children)
      )

    ch_after2 = terminate_children(ch_after, state(state, :name))

    case start_children(ch_after2, state(state, :name)) do
      {:ok, ch_after3} ->
        {:ok, state(state, children: ch_after3 ++ ch_before)}

      {:error, ch_after3, reason} ->
        n_child = child(child, pid: restarting(child(child, :pid)))
        n_state = state(state, children: ch_after3 ++ ch_before)
        {:try_again, reason, replace_child(n_child, n_state)}
    end
  end

  defp restart(:one_for_all, child, state) do
    children1 =
      del_child(
        child(child, :pid),
        state(state, :children)
      )

    children2 =
      terminate_children(
        children1,
        state(state, :name)
      )

    case start_children(children2, state(state, :name)) do
      {:ok, n_chs} ->
        {:ok, state(state, children: n_chs)}

      {:error, n_chs, reason} ->
        n_child = child(child, pid: restarting(child(child, :pid)))
        n_state = state(state, children: n_chs)
        {:try_again, reason, replace_child(n_child, n_state)}
    end
  end

  defp restarting(pid) when is_pid(pid) do
    {:restarting, pid}
  end

  defp restarting(r_pid) do
    r_pid
  end

  defp terminate_children(children, sup_name) do
    terminate_children(children, sup_name, [])
  end

  defp terminate_children(
         [
           child = child(restart_type: :temporary)
           | children
         ],
         sup_name,
         res
       ) do
    do_terminate(child, sup_name)
    terminate_children(children, sup_name, res)
  end

  defp terminate_children([child | children], sup_name, res) do
    n_child = do_terminate(child, sup_name)
    terminate_children(children, sup_name, [n_child | res])
  end

  defp terminate_children([], _sup_name, res) do
    res
  end

  defp do_terminate(child, sup_name) when is_pid(child(child, :pid)) do
    case shutdown(child(child, :pid), child(child, :shutdown)) do
      :ok ->
        :ok

      {:error, :normal}
      when not (child(child, :restart_type) === :permanent or
                    (is_tuple(child(child, :restart_type)) and
                       tuple_size(child(child, :restart_type)) == 2 and
                       :erlang.element(
                         1,
                         child(child, :restart_type)
                       ) === :permanent)) ->
        :ok

      {:error, otherreason} ->
        report_error(:shutdown_error, otherreason, child, sup_name)
    end

    child(child, pid: :undefined)
  end

  defp do_terminate(child(pid: {:delayed_restart, t_ref}) = child, _sup_name) do
    Process.cancel_timer(t_ref)
    child(child, pid: :undefined)
  end

  defp do_terminate(child, _sup_name) do
    child(child, pid: :undefined)
  end

  defp shutdown(pid, :brutal_kill) do
    case monitor_child(pid) do
      :ok ->
        Process.exit(pid, :kill)

        receive do
          {:DOWN, _mref, :process, ^pid, :killed} ->
            :ok

          {:DOWN, _mref, :process, ^pid, otherreason} ->
            {:error, otherreason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp shutdown(pid, time) do
    case monitor_child(pid) do
      :ok ->
        Process.exit(pid, :shutdown)

        receive do
          {:DOWN, _mref, :process, ^pid, :shutdown} ->
            :ok

          {:DOWN, _mref, :process, ^pid, otherreason} ->
            {:error, otherreason}
        after
          time ->
            Process.exit(pid, :kill)

            receive do
              {:DOWN, _mref, :process, ^pid, otherreason} ->
                {:error, otherreason}
            end
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp monitor_child(pid) do
    Process.monitor(pid)
    Process.unlink(pid)

    receive do
      {:EXIT, ^pid, reason} ->
        receive do
          {:DOWN, _, :process, ^pid, _} ->
            {:error, reason}
        end
    after
      0 ->
        :ok
    end
  end

  defp terminate_dynamic_children(child, dynamics, sup_name) do
    {pids, e_stack0} =
      monitor_dynamic_children(
        child,
        dynamics
      )

    sz = :sets.size(pids)

    e_stack =
      case child(child, :shutdown) do
        :brutal_kill ->
          :sets.fold(
            fn p, _ ->
              Process.exit(p, :kill)
            end,
            :ok,
            pids
          )

          wait_dynamic_children(child, pids, sz, :undefined, e_stack0)

        :infinity ->
          :sets.fold(
            fn p, _ ->
              Process.exit(p, :shutdown)
            end,
            :ok,
            pids
          )

          wait_dynamic_children(child, pids, sz, :undefined, e_stack0)

        time ->
          :sets.fold(
            fn p, _ ->
              Process.exit(p, :shutdown)
            end,
            :ok,
            pids
          )

          t_ref = :erlang.start_timer(time, self(), :kill)
          wait_dynamic_children(child, pids, sz, t_ref, e_stack0)
      end

    :dict.fold(
      fn reason, ls, _ ->
        report_error(:shutdown_error, reason, child(child, pid: ls), sup_name)
      end,
      :ok,
      e_stack
    )
  end

  defp monitor_dynamic_children(child(restart_type: :temporary), dynamics) do
    :sets.fold(
      fn p, {pids, e_stack} ->
        case monitor_child(p) do
          :ok ->
            {:sets.add_element(p, pids), e_stack}

          {:error, :normal} ->
            {pids, e_stack}

          {:error, reason} ->
            {pids, :dict.append(reason, p, e_stack)}
        end
      end,
      {:sets.new(), :dict.new()},
      dynamics
    )
  end

  defp monitor_dynamic_children(child(restart_type: r_type), dynamics) do
    :dict.fold(
      fn
        p, _, {pids, e_stack} when is_pid(p) ->
          case monitor_child(p) do
            :ok ->
              {:sets.add_element(p, pids), e_stack}

            {:error, :normal}
            when not (r_type === :permanent or
                          (is_tuple(r_type) and tuple_size(r_type) == 2 and
                             :erlang.element(
                               1,
                               r_type
                             ) === :permanent)) ->
              {pids, e_stack}

            {:error, reason} ->
              {pids, :dict.append(reason, p, e_stack)}
          end

        {:restarting, _}, _, {pids, e_stack} ->
          {pids, e_stack}
      end,
      {:sets.new(), :dict.new()},
      dynamics
    )
  end

  defp wait_dynamic_children(_child, _pids, 0, :undefined, e_stack) do
    e_stack
  end

  defp wait_dynamic_children(_child, _pids, 0, t_ref, e_stack) do
    Process.cancel_timer(t_ref)

    receive do
      {:timeout, ^t_ref, :kill} ->
        e_stack
    after
      0 ->
        e_stack
    end
  end

  defp wait_dynamic_children(child(shutdown: :brutal_kill) = child, pids, sz, t_ref, e_stack) do
    receive do
      {:DOWN, _mref, :process, pid, :killed} ->
        wait_dynamic_children(child, :sets.del_element(pid, pids), sz - 1, t_ref, e_stack)

      {:DOWN, _mref, :process, pid, reason} ->
        wait_dynamic_children(
          child,
          :sets.del_element(pid, pids),
          sz - 1,
          t_ref,
          :dict.append(reason, pid, e_stack)
        )
    end
  end

  defp wait_dynamic_children(child(restart_type: r_type) = child, pids, sz, t_ref, e_stack) do
    receive do
      {:DOWN, _mref, :process, pid, :shutdown} ->
        wait_dynamic_children(child, :sets.del_element(pid, pids), sz - 1, t_ref, e_stack)

      {:DOWN, _mref, :process, pid, :normal}
      when not (r_type === :permanent or
                    (is_tuple(r_type) and tuple_size(r_type) == 2 and
                       :erlang.element(
                         1,
                         r_type
                       ) === :permanent)) ->
        wait_dynamic_children(child, :sets.del_element(pid, pids), sz - 1, t_ref, e_stack)

      {:DOWN, _mref, :process, pid, reason} ->
        wait_dynamic_children(
          child,
          :sets.del_element(pid, pids),
          sz - 1,
          t_ref,
          :dict.append(reason, pid, e_stack)
        )

      {:timeout, ^t_ref, :kill} ->
        :sets.fold(
          fn p, _ ->
            Process.exit(p, :kill)
          end,
          :ok,
          pids
        )

        wait_dynamic_children(child, pids, sz - 1, :undefined, e_stack)
    end
  end

  defp save_child(
         child(
           restart_type: :temporary,
           mfargs: {m, f, _}
         ) = child,
         state(children: children) = state
       ) do
    state(state,
      children: [
        child(child, mfargs: {m, f, :undefined})
        | children
      ]
    )
  end

  defp save_child(child, state(children: children) = state) do
    state(state, children: [child | children])
  end

  defp save_dynamic_child(:temporary, pid, _, state(dynamics: dynamics) = state) do
    state(state,
      dynamics:
        :sets.add_element(
          pid,
          dynamics_db(:temporary, dynamics)
        )
    )
  end

  defp save_dynamic_child(restart_type, pid, args, state(dynamics: dynamics) = state) do
    state(state, dynamics: :dict.store(pid, args, dynamics_db(restart_type, dynamics)))
  end

  defp dynamics_db(:temporary, :undefined) do
    :sets.new()
  end

  defp dynamics_db(_, :undefined) do
    :dict.new()
  end

  defp dynamics_db(_, dynamics) do
    dynamics
  end

  defp dynamic_child_args(pid, dynamics) do
    case :sets.is_set(dynamics) do
      true ->
        {:ok, :undefined}

      false ->
        :dict.find(pid, dynamics)
    end
  end

  defp state_del_child(child(pid: pid, restart_type: :temporary), state)
       when state(state, :strategy) === :simple_one_for_one do
    n_dynamics =
      :sets.del_element(
        pid,
        dynamics_db(
          :temporary,
          state(state, :dynamics)
        )
      )

    state(state, dynamics: n_dynamics)
  end

  defp state_del_child(child(pid: pid, restart_type: r_type), state)
       when state(state, :strategy) === :simple_one_for_one do
    n_dynamics =
      :dict.erase(
        pid,
        dynamics_db(r_type, state(state, :dynamics))
      )

    state(state, dynamics: n_dynamics)
  end

  defp state_del_child(child, state) do
    n_children =
      del_child(
        child(child, :name),
        state(state, :children)
      )

    state(state, children: n_children)
  end

  defp del_child(name, [ch = child(pid: {:restarting, _}) | _] = chs)
       when child(ch, :name) === name do
    chs
  end

  defp del_child(name, [ch | chs])
       when child(ch, :name) === name and
              child(ch, :restart_type) === :temporary do
    chs
  end

  defp del_child(name, [ch | chs]) when child(ch, :name) === name do
    [child(ch, pid: :undefined) | chs]
  end

  defp del_child(pid, [ch | chs])
       when child(ch, :pid) === pid and
              child(ch, :restart_type) === :temporary do
    chs
  end

  defp del_child(pid, [ch | chs]) when child(ch, :pid) === pid do
    [child(ch, pid: :undefined) | chs]
  end

  defp del_child(name, [ch | chs]) do
    [ch | del_child(name, chs)]
  end

  defp del_child(_, []) do
    []
  end

  defp split_child(name, chs) do
    split_child(name, chs, [])
  end

  defp split_child(name, [ch | chs], after__)
       when child(ch, :name) === name do
    {:lists.reverse([child(ch, pid: :undefined) | after__]), chs}
  end

  defp split_child(pid, [ch | chs], after__)
       when child(ch, :pid) === pid do
    {:lists.reverse([child(ch, pid: :undefined) | after__]), chs}
  end

  defp split_child(name, [ch | chs], after__) do
    split_child(name, chs, [ch | after__])
  end

  defp split_child(_, [], after__) do
    {:lists.reverse(after__), []}
  end

  defp get_child(name, state) do
    get_child(name, state, false)
  end

  defp get_child(pid, state, allow_pid) when allow_pid and is_pid(pid) do
    get_dynamic_child(pid, state)
  end

  defp get_child(name, state, _) do
    :lists.keysearch(name, child(:name), state(state, :children))
  end

  defp get_dynamic_child(
         pid,
         state(children: [child], dynamics: dynamics)
       ) do
    dynamics_db =
      dynamics_db(
        child(child, :restart_type),
        dynamics
      )

    case is_dynamic_pid(pid, dynamics_db) do
      true ->
        {:value, child(child, pid: pid)}

      false ->
        r_pid = restarting(pid)

        case {is_dynamic_pid(r_pid, dynamics_db), :erlang.is_process_alive(pid)} do
          {true, _} ->
            {:value, child(child, pid: r_pid)}

          {false, false} ->
            {:value, child}

          {false, true} ->
            false
        end
    end
  end

  defp is_dynamic_pid(pid, dynamics) do
    case :sets.is_set(dynamics) do
      true ->
        :sets.is_element(pid, dynamics)

      false ->
        :dict.is_key(pid, dynamics)
    end
  end

  defp replace_child(child, state) do
    chs = do_replace_child(child, state(state, :children))
    state(state, children: chs)
  end

  defp do_replace_child(child, [ch | chs])
       when child(ch, :name) === child(child, :name) do
    [child | chs]
  end

  defp do_replace_child(child, [ch | chs]) do
    [ch | do_replace_child(child, chs)]
  end

  defp remove_child(child, state) do
    chs = :lists.keydelete(child(child, :name), child(:name), state(state, :children))
    state(state, children: chs)
  end

  defp do_init(sup_name, type, start_spec, mod, args) do
    case (try do
            init_state(sup_name, type, mod, args)
          catch
            :error, e -> {:EXIT, {e, __STACKTRACE__}}
            :exit, e -> {:EXIT, e}
            e -> e
          end) do
      {:ok, state}
      when state(state, :strategy) === :simple_one_for_one ->
        init_dynamic(state, start_spec)

      {:ok, state} ->
        init_children(state, start_spec)

      error ->
        {:stop, {:supervisor_data, error}}
    end
  end

  defp init_state(sup_name, {strategy, max_intensity, period}, mod, args) do
    valid_strategy(strategy)
    valid_intensity(max_intensity)
    valid_period(period)

    {:ok,
     state(
       name: supname(sup_name, mod),
       strategy: strategy,
       intensity: max_intensity,
       period: period,
       module: mod,
       args: args
     )}
  end

  defp init_state(_sup_name, type, _, _) do
    {:invalid_type, type}
  end

  defp valid_strategy(:simple_one_for_one) do
    true
  end

  defp valid_strategy(:one_for_one) do
    true
  end

  defp valid_strategy(:one_for_all) do
    true
  end

  defp valid_strategy(:rest_for_one) do
    true
  end

  defp valid_strategy(what) do
    throw({:invalid_strategy, what})
  end

  defp valid_intensity(max) when is_integer(max) and max >= 0 do
    true
  end

  defp valid_intensity(what) do
    throw({:invalid_intensity, what})
  end

  defp valid_period(period)
       when is_integer(period) and
              period > 0 do
    true
  end

  defp valid_period(what) do
    throw({:invalid_period, what})
  end

  defp supname(:self, mod) do
    {self(), mod}
  end

  defp supname(n, _) do
    n
  end

  defp check_startspec(children) do
    check_startspec(children, [])
  end

  defp check_startspec([child_spec | t], res) do
    case check_childspec(child_spec) do
      {:ok, child} ->
        case :lists.keymember(child(child, :name), child(:name), res) do
          true ->
            {:duplicate_child_name, child(child, :name)}

          false ->
            check_startspec(t, [child | res])
        end

      error ->
        error
    end
  end

  defp check_startspec([], res) do
    {:ok, :lists.reverse(res)}
  end

  defp check_childspec({name, func, restart_type, shutdown, child_type, mods}) do
    check_childspec(name, func, restart_type, shutdown, child_type, mods)
  catch
    :error, e -> {:EXIT, {e, __STACKTRACE__}}
    :exit, e -> {:EXIT, e}
    e -> e
  end

  defp check_childspec(x) do
    {:invalid_child_spec, x}
  end

  defp check_childspec(name, func, restart_type, shutdown, child_type, mods) do
    valid_name(name)
    valid_func(func)
    valid_restart_type(restart_type)
    valid_child_type(child_type)
    valid_shutdown(shutdown, child_type)
    valid_mods(mods)

    {:ok,
     child(
       name: name,
       mfargs: func,
       restart_type: restart_type,
       shutdown: shutdown,
       child_type: child_type,
       modules: mods
     )}
  end

  defp valid_child_type(:supervisor) do
    true
  end

  defp valid_child_type(:worker) do
    true
  end

  defp valid_child_type(what) do
    throw({:invalid_child_type, what})
  end

  defp valid_name(_Name) do
    true
  end

  defp valid_func({m, f, a}) when is_atom(m) and is_atom(f) and is_list(a) do
    true
  end

  defp valid_func(func) do
    throw({:invalid_mfa, func})
  end

  defp valid_restart_type(:permanent) do
    true
  end

  defp valid_restart_type(:temporary) do
    true
  end

  defp valid_restart_type(:transient) do
    true
  end

  defp valid_restart_type(:intrinsic) do
    true
  end

  defp valid_restart_type({:permanent, delay}) do
    valid_delay(delay)
  end

  defp valid_restart_type({:intrinsic, delay}) do
    valid_delay(delay)
  end

  defp valid_restart_type({:transient, delay}) do
    valid_delay(delay)
  end

  defp valid_restart_type(restart_type) do
    throw({:invalid_restart_type, restart_type})
  end

  defp valid_delay(delay) when is_number(delay) and delay >= 0 do
    true
  end

  defp valid_delay(what) do
    throw({:invalid_delay, what})
  end

  defp valid_shutdown(shutdown, _)
       when is_integer(shutdown) and
              shutdown > 0 do
    true
  end

  defp valid_shutdown(:infinity, _) do
    true
  end

  defp valid_shutdown(:brutal_kill, _) do
    true
  end

  defp valid_shutdown(shutdown, _) do
    throw({:invalid_shutdown, shutdown})
  end

  defp valid_mods(:dynamic) do
    true
  end

  defp valid_mods(mods) when is_list(mods) do
    :lists.foreach(
      fn mod ->
        case is_atom(mod) do
          true -> :ok
          false -> throw({:invalid_module, mod})
        end
      end,
      mods
    )
  end

  defp valid_mods(mods) do
    throw({:invalid_modules, mods})
  end

  defp add_restart(state) do
    add_restart(state, _is_clean_retry = false)
  end

  defp add_restart(state, is_clean_retry) do
    max_r = state(state, :intensity)
    p = state(state, :period)
    r = state(state, :restarts)
    now = :os.timestamp()

    r1 =
      case is_clean_retry do
        true ->
          delete_old_restarts(r, now, p)

        false ->
          delete_old_restarts([now | r], now, p)
      end

    state1 = state(state, restarts: r1)

    case length(r1) do
      count when count <= max_r ->
        {:ok, state1}

      _ ->
        {:terminate, state1}
    end
  end

  defp delete_old_restarts([], _, _) do
    []
  end

  defp delete_old_restarts([r | restarts], now, period) do
    case is_in_period(r, now, period) do
      true ->
        [r | delete_old_restarts(restarts, now, period)]

      _ ->
        []
    end
  end

  defp is_in_period(time, now, period) do
    div(:timer.now_diff(now, time), 1_000_000) <= period
  end

  defp report_error(error, reason, child, sup_name) do
    error_msg = [
      {:supervisor, sup_name},
      {:errorContext, error},
      {:reason, reason},
      {:offender, extract_child(child)}
    ]

    :error_logger.error_report(:supervisor_report, error_msg)
  end

  defp extract_child(child) when is_list(child(child, :pid)) do
    [
      {:nb_children, length(child(child, :pid))}
      | extract_child_common(child)
    ]
  end

  defp extract_child(child) do
    [{:pid, child(child, :pid)} | extract_child_common(child)]
  end

  defp extract_child_common(child) do
    [
      {:id, child(child, :name)},
      {:mfargs, child(child, :mfargs)},
      {:restart_type, child(child, :restart_type)},
      {:shutdown, child(child, :shutdown)},
      {:child_type, child(child, :child_type)}
    ]
  end

  defp report_progress(child, sup_name) do
    progress = [{:supervisor, sup_name}, {:started, extract_child(child)}]
    :error_logger.info_report(:progress, progress)
  end
end
