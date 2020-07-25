package org.example;

import java.util.Comparator;

public class TopNComparator implements Comparator<String> {

    @Override
    public int compare(String o1, String o2) {
        String[] split1 = o1.split(":");
        String[] split2 = o2.split(":");
        System.out.println(o1+"======="+o2);
        if(!split1[0].equals(split2[0])){
            if(split1[1].equals(split2[1])){
                return split1[0].compareTo(split2[0]);
            }
            return Integer.parseInt(split1[1])-Integer.parseInt(split2[1]);
        }
        return o1.compareTo(o2);
    }
}
