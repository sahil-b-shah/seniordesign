package Manager;

import java.util.HashSet;
import java.util.Set;

public class Job {
	private String jobId;
	private StringBuilder resultSet;
	private int numberNodes;
	private int nodesFinished;
	private Set<Integer> failureNodes;
	
	public Job(String jobId, String resultSet, int numNodes) {
		this.jobId = jobId;
		this.resultSet = new StringBuilder();
		this.resultSet.append(resultSet);
		this.numberNodes = numNodes;
		this.nodesFinished = 0;
		this.failureNodes = new HashSet<Integer>();
	}
	
	public Job(String jobId, int numNodes) {
		this.jobId = jobId;
		this.resultSet = new StringBuilder();
		this.numberNodes = numNodes;
		this.nodesFinished = 0;
		this.failureNodes = new HashSet<Integer>();
	}
	
	public String getResultSet() {
		return this.resultSet.toString();
	}
	
	public void addToResultSet(String result, int nodeNum) {
		if (result != null && !result.isEmpty()) {
			this.resultSet.append(result);
		}
		this.nodesFinished++;
	}
	
	public int numberNodesFinished() {
		return this.nodesFinished;
	}
	
	public boolean jobFinished() {
		return this.nodesFinished == numberNodes;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	public void addFailureNode(int nodeNum) {
		this.nodesFinished++;
		this.failureNodes.add(nodeNum);
	}
}
