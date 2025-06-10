package org.name.domain;

public class CoordinateTuple {
    Integer row;
    Integer column;

    public CoordinateTuple(Integer row, Integer column) {
        this.row = row;
        this.column = column;
    }

    public Integer row() {
        return row;
    }

    public Integer column() {
        return column;
    }

    @Override
    public String toString() {
        return "'(" + row + "," + column + ")'";
    }
}
