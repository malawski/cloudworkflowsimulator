class TaskLog
  def initialize(id, workflow, task_id, vm, started, finished, result)
    @workflow = workflow
    @id = id
    @task_id = task_id
    @vm = vm
    @started = started
    @finished = finished
    @result = result
  end

  attr_reader :id, :workflow, :task_id, :vm, :started, :finished, :result
end

class TransferLog
  def initialize(id, vm, started, finished, direction, job_id, file_id)
    @id = id
    @vm = vm
    @started = started
    @finished = finished
    @direction = direction
    @job_id = job_id
    @file_id = file_id
  end

  attr_reader :id, :vm, :started, :finished, :direction, :job_id, :file_id
end

class VMLog
  def initialize(id, started, finished, cores, price_for_billing_unit)
    @id = id
    @started = started
    @finished = finished
    @cores = cores
    @price_for_billing_unit = price_for_billing_unit
  end

  attr_reader :id, :started, :finished
end

class Workflow
  def initialize(id, priority)
    @id = id
    @priority = priority
  end

  attr_reader :id, :priority
end

class StorageState
  def initialize(time, readers_number, writers_number, read_speed, write_speed)
    @time = time
    @readers_number = readers_number
    @writers_number = writers_number
    @read_speed = read_speed
    @write_speed = write_speed
  end

  attr_reader :time, :readers_number, :writers_number, :read_speed, :write_speed
end

def read_log(file_content)
  lines = file_content.split(/\n/)

  # skip experiment settings
  current_line = 1

  vm_number = lines[current_line].to_i
  current_line += 1

  vms = Hash.new

  for i in 0...vm_number
    vm_info = lines[current_line].split

    vm = VMLog.new(vm_info[0], vm_info[1].to_f, vm_info[2].to_f, vm_info[3].to_f, vm_info[4].to_f)
    vms[vm.id] = vm

    current_line += 1
  end

  workflows_number = lines[current_line].to_i
  current_line += 1

  workflows = Hash.new

  for i in 0...workflows_number
    workflow_info = lines[current_line].split
    workflow = Workflow.new(workflow_info[0], workflow_info[1].to_i)
    workflows[workflow.id] = workflow
    current_line += 1
  end

  tasks_number = lines[current_line].to_i
  current_line += 1

  tasks = []

  for i in 0...tasks_number
    task_info = lines[current_line].split
    task = TaskLog.new(task_info[0], task_info[1], task_info[2], task_info[3], task_info[4].to_f, task_info[5].to_f, task_info[6])
    tasks.push(task)
    current_line += 1
  end

  transfers_number = lines[current_line].to_i
  current_line += 1

  transfers = []

  for i in 0...transfers_number
    transfer_info = lines[current_line].split
    transfer = TransferLog.new(transfer_info[0], transfer_info[1], transfer_info[2].to_f, transfer_info[3].to_f, transfer_info[4], transfer_info[5], transfer_info[6])
    transfers.push(transfer)
    current_line += 1
  end

  storage_states_number = lines[current_line].to_i
  current_line += 1

  storage_states = []
  for i in 0...storage_states_number
    storage_state_info = lines[current_line].split
    storage_state = StorageState.new(storage_state_info[0].to_f, storage_state_info[1].to_i, storage_state_info[2].to_i, storage_state_info[3].to_f, storage_state_info[4].to_f)
    storage_states.push(storage_state)
    current_line += 1
  end

  return {
    :vms => vms,
    :workflows => workflows,
    :tasks => tasks,
    :transfers => transfers,
    :storage_states => storage_states,
  }
end

def read_log_from_file(filename)
  file_content = `cat #{filename}`
  return read_log(file_content)
end
