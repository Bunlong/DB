public class ExecutionTimer {

	private long start;
	private long end;

	public ExecutionTimer() {
		reset();
	}

	public void start() {
		start = System.currentTimeMillis();
	}

	public void end() {
		end = System.currentTimeMillis();
	}

	public long duration(){
		return (end - start);
	}

	public void reset() {
		start = 0;  
		end = 0;
	}
}
