package Manager;

import java.util.HashSet;
import java.util.Set;

public class Job {
	private String jobId;
	private StringBuilder resultSet;
	private int numberNodes;
	private Set<Integer> nodesFinished;
	private Set<Integer> failureNodes;
	
	public Job(String jobId, String resultSet, int numNodes) {
		this.jobId = jobId;
		this.resultSet = new StringBuilder();
		this.resultSet.append(resultSet);
		this.numberNodes = numNodes;
		this.nodesFinished = new HashSet<Integer>();
		this.failureNodes = new HashSet<Integer>();
	}
	
	public Job(String jobId, int numNodes) {
		this.jobId = jobId;
		this.resultSet = new StringBuilder();
		this.numberNodes = numNodes;
		this.nodesFinished = new HashSet<Integer>();
		this.failureNodes = new HashSet<Integer>();
	}
	
	public String getResultSet() {
		return this.resultSet.toString();
	}
	
	public void addToResultSet(String result, int nodeNum) {
		if (this.nodesFinished.contains(nodeNum)) {
			return;
		}
		if (result != null) {
			this.resultSet.append(result);
		}
		this.nodesFinished.add(nodeNum);
	}
	
	public int numberNodesFinished() {
		return this.nodesFinished.size();
	}
	
	public boolean jobFinished() {
		return this.nodesFinished.size() == numberNodes;
	}
	
	public String getJobId() {
		return this.jobId;
	}
	
	public void addFailureNode(int nodeNum) {
		if (this.nodesFinished.contains(nodeNum)) {
			return;
		}
		this.nodesFinished.add(nodeNum);
		this.failureNodes.add(nodeNum);
	}
}
