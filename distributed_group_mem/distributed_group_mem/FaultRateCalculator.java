package distributed_group_mem;


//SIMPLE CLASS USED TO GET FAULT RATE
public class FaultRateCalculator {

	public static int falseDetections;
	public static int notfalseDetections;

	public int getfaultRatePositiveValue () {
		System.out.println("falseDetections:" + String.valueOf(falseDetections));
		System.out.println("totalDetections:" + String.valueOf(falseDetections+notfalseDetections));
		return (falseDetections/(falseDetections+notfalseDetections));
	}
}
