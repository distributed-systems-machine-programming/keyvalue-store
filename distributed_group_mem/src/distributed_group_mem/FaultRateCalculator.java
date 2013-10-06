package distributed_group_mem;

public class FaultRateCalculator {

	public static int falseDetections;
	public static int notfalseDetections;

	public int getfaultRatePositiveValue () {
		return (falseDetections/(falseDetections+notfalseDetections));
	}
}
