package org.name.domain;

import java.util.Set;

public class SQAEntry {
    String id;
    Integer annotator;
    Integer position;
    String question;
    String table_file;
    Set<CoordinateTuple> answer_coordinates;

    public SQAEntry(String id, Integer annotator, Integer position, String question,
                    String table_file, Set<CoordinateTuple> answer_coordinates) {
        this.id = id;
        this.annotator = annotator;
        this.position = position;
        this.question = question;
        this.table_file = table_file;
        this.answer_coordinates = answer_coordinates;
    }
}
