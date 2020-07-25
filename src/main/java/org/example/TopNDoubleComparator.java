package org.example;

import java.util.Comparator;

public class TopNDoubleComparator implements Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
        String[] split1 = o1.split(":");
        String[] split2 = o2.split(":");
        if(split1.length!=2&&split2.length!=2){
            return split1[0].compareTo(split2[0]);
        }else if(split1.length!=2){
            return -1;
        }else if(split2.length!=2){
            return 1;
        }
        if(!split1[0].equals(split2[0])){
            if(split1[1].equals(split2[1])){
                return split1[0].compareTo(split2[0]);
            }
            double v = Double.parseDouble(split1[1]) - Double.parseDouble(split2[1]);
            if(v>0){
                return 1;
            }else if(v<0){
                return -1;
            }
            return 0;
        }
        return o1.compareTo(o2);
    }
}
