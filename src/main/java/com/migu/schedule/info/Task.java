package com.migu.schedule.info;

public class Task {
	private int taskId;
	private int consumption;
	private int nodeId;
	public int getTaskId() {
		return taskId;
	}
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	public int getConsumption() {
		return consumption;
	}
	public void setConsumption(int consumption) {
		this.consumption = consumption;
	}
	
	public int getNodeId() {
		return nodeId;
	}
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	@Override
	public String toString() {
		return "Task [taskId=" + taskId + ", consumption=" + consumption + ", nodeId=" + nodeId + "]";
	}
	
}
