
public class Driver {

    public static void main(String[] args) throws Exception {

        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        //args0: dir of transition.txt
        //args1: dir of PageRank.txt
        //args2: dir of unitMultiplication result
        //args3: times of convergence
        String transitionMatrix = args[0]; // the transition Matrix is always the same
        String prMatrix = args[1]; // PR n-1
        String unitState = args[2]; // output from first mapreduce, key toId value cellsubProb
        String beta = args[4]; // input for beta
        int count = Integer.parseInt(args[3]);
        for(int i=0;  i<count;  i++) {
            // /prMatrix0, /prMatrix1, ...
            // /unitState0, /unitState1, ...
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i};
            multiplication.main(args1);
            // when considering dead end and spider traps, the unitSum need previous PR
            // String[] args2 = {unitState + i, prMatrix+(i+1)};
            String[] args2 = {unitState + i, prMatrix+i, beta, prMatrix+(i+1)};
            sum.main(args2);
        }
    }
}
