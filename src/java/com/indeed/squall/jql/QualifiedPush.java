package com.indeed.squall.jql;

import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author jwolfe
 */
public class QualifiedPush {
    private static final Logger log = Logger.getLogger(QualifiedPush.class);

    public final String sessionName;
    public final List<String> pushes;

    public QualifiedPush(String sessionName, List<String> pushes) {
        this.sessionName = sessionName;
        this.pushes = pushes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QualifiedPush that = (QualifiedPush) o;

        if (!pushes.equals(that.pushes)) return false;
        if (!sessionName.equals(that.sessionName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = sessionName.hashCode();
        result = 31 * result + pushes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "com.indeed.squall.jql.QualifiedPush{" +
                "sessionName='" + sessionName + '\'' +
                ", pushes=" + pushes +
                '}';
    }
}
