package com.tresata.ganitha.mahout;

import org.apache.mahout.math.Vector;

class VectorUtil {

    private VectorUtil() {
    }

    public static Vector cloneVector(Vector vector) {
        return vector.clone();
    }

}