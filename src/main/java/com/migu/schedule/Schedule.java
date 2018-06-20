package com.migu.schedule;


import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.Task;
import com.migu.schedule.info.TaskInfo;

/*
*类名和方法不能修改
 */
public class Schedule {
	private Map<Integer, List<Task>> tasks = new ConcurrentHashMap<Integer, List<Task>>();
	
    public int init() {
    	tasks.clear();
    	List<Task> waitingList = new CopyOnWriteArrayList<Task>();
    	tasks.put(-1, waitingList);
        return ReturnCodeKeys.E001;
    }

    public int registerNode(int nodeId) {
        if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}
        if (tasks.containsKey(nodeId)) {
        	return ReturnCodeKeys.E005;
		}
        List<Task> nodeList = new CopyOnWriteArrayList<Task>(); 
        tasks.put(nodeId, nodeList);
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
    	if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}
    	if (tasks.containsKey(nodeId)) {
			List<Task> list = tasks.get(nodeId);
			tasks.get(-1).addAll(list);
			tasks.remove(nodeId);
			return ReturnCodeKeys.E006;
		} else {
			return ReturnCodeKeys.E007;
		}
    }


    public int addTask(int taskId, int consumption) {
    	if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}
    	Task task = getTaskById(taskId);
    	if (task != null) {
    		return ReturnCodeKeys.E010;
		}
    	Task newTask = new Task();
    	newTask.setTaskId(taskId);
    	newTask.setConsumption(consumption);
    	newTask.setNodeId(-1);
    	tasks.get(-1).add(newTask);
        return ReturnCodeKeys.E008;
    }


    

	public int deleteTask(int taskId) {
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}
        Task task = getTaskById(taskId);
        if (task != null) {
			tasks.get(task.getNodeId()).remove(task);
			return ReturnCodeKeys.E011;
		} else {
			return ReturnCodeKeys.E012;
		}
    }


    public int scheduleTask(int threshold) {
    	if (threshold <= 0) {
			return ReturnCodeKeys.E002;
		}
    	List<Task> waitingList = tasks.get(-1);
    	for (Task task : waitingList) {
    		
		}
        return ReturnCodeKeys.E013;
    }
    
	private int getNodeConsumption(Integer nodeId) {
		int nodeConsumption = 0;
		List<Task> list = tasks.get(nodeId);
		for (Task task : list) {
			nodeConsumption += task.getConsumption();
		}
		return nodeConsumption;
	}

	public int queryTaskStatus(List<TaskInfo> tasks) {
        if (tasks == null) {
			return ReturnCodeKeys.E016;
		}
        tasks.clear();
        getAllTasks(tasks);
        Collections.sort(tasks,new Comparator<TaskInfo>() {
			public int compare(TaskInfo o1, TaskInfo o2) {
				int i = o1.getTaskId() - o2.getTaskId();
				if (i == 0) {
					return o1.getNodeId() - o2.getNodeId();
				}
				return i;
			}
		});
        return ReturnCodeKeys.E015;
    }
 
	private void getAllTasks(List<TaskInfo> newTasks) {
		for (Entry<Integer, List<Task>> entry: tasks.entrySet()) {
			List<Task> nodeTasks = entry.getValue();
			for (Task task : nodeTasks) {
				TaskInfo taskInfo = new TaskInfo();
				taskInfo.setTaskId(task.getTaskId());
				taskInfo.setNodeId(task.getNodeId());
				newTasks.add(taskInfo);
			}
		}
	}

	private Task getTaskById(int taskId) {
		for (Entry<Integer, List<Task>> entry: tasks.entrySet()) {
			List<Task> nodeTasks = entry.getValue();
			for (Task task : nodeTasks) {
				if (taskId == task.getTaskId()) {
					return task;
				}
			}
		}
		return null;
	}
	
}
