
public class Driver {

    public static void main(String[] args) throws Exception {


        // UnitSum and UnitMultiplication are static class, no need to create an instance
        // UnitMultiplication multiplication = new UnitMultiplication();
        // UnitSum sum = new UnitSum();

        //args0: dir of transition.txt
        //args1: dir of PageRank.txt
        //args2: dir where to save the unitMultiplication result
        //args3: times of convergence
        //args4: the teleport parameter beta
        String transitionMatrix = args[0]; // the transition Matrix is always the same
        String prMatrix = args[1]; // PR N-1
        String unitState = args[2]; // output from first mapreduce, key: toId value cellsubProb
        String beta = args[4]; // input for beta range 0 ~1.0
        int count = Integer.parseInt(args[3]);
        for(int i=0;  i<count;  i++) {
            // dir /prMatrix0, /prMatrix1, ...
            // dir /unitState0, /unitState1, ...
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i};
            UnitMultiplication.main(args1);
            // when considering dead end and spider traps, the unitSum need previous PR
            String[] args2 = {unitState + i, prMatrix+i, beta, prMatrix+(i+1)};
            UnitSum.main(args2);
            /*
            will receive warning if using sum.main(args2)
            only use the static class no need to create an object
            warning static member UnitSum.main accessed via instance reference
             */
        }
    }
}
