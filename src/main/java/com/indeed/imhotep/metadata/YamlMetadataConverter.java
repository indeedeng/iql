package com.indeed.imhotep.metadata;

import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.FieldsYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;

import java.util.LinkedHashMap;

/**
 * This is a very inefficient way to convert those classes, and it encourages
 * code duplication
 * a better way would be to merge datasetMetadata with datasetYaml with (json) Dataset
 * since all those classes are basically container classes.
 * Created by ivana on 7/8/15.
 */
public class YamlMetadataConverter {
    private YamlMetadataConverter(){
    }

    public static DatasetMetadata convertDataset(DatasetYaml datasetYaml){
        DatasetMetadata datasetMetadata = new DatasetMetadata(datasetYaml.getName());
        datasetMetadata.setDescription(datasetMetadata.getDescription());
        datasetMetadata.type = convertDatasetType(datasetYaml.getType());

        LinkedHashMap<String, FieldMetadata> fields = datasetMetadata.fields;
        LinkedHashMap<String, MetricMetadata> metrics = datasetMetadata.metrics;

        FieldsYaml fieldsYaml[] = datasetYaml.getFields();
        MetricsYaml metricsYaml[] = datasetYaml.getMetrics();

        for (FieldsYaml field: fieldsYaml){
            fields.put(field.getName(), convertFieldMetadata(field));
        }

        for (MetricsYaml metric: metricsYaml){
            metrics.put(metric.getName(), convertMetricMetadata(metric));
        }
        return datasetMetadata;
    }

    public static FieldMetadata convertFieldMetadata(FieldsYaml fieldsYaml){
        if (fieldsYaml==null){
            return null;
        }
        FieldMetadata fieldMetadata= new FieldMetadata(fieldsYaml.getName(), convertFieldType(fieldsYaml.getType()));
        fieldMetadata.setDescription(fieldsYaml.getDescription());
        fieldMetadata.setFriendlyName(fieldsYaml.getFriendlyName());
        fieldMetadata.setHidden(fieldsYaml.getHidden());
        return fieldMetadata;
    }

    public static FieldType convertFieldType(String fieldType){
        if(fieldType!=null) {
            if (fieldType.equalsIgnoreCase("Integer")) {
                return FieldType.Integer;
            } else {
                return FieldType.String;
            }
        }
        else{
            return FieldType.String;
        }
    }

    public static MetricMetadata convertMetricMetadata(MetricsYaml metricYaml){
        if (metricYaml==null){
            return null;
        }
        MetricMetadata metricMetadata= new MetricMetadata(metricYaml.getName());
        metricMetadata.setDescription(metricYaml.getDescription());
        metricMetadata.setFriendlyName(metricYaml.getFriendlyName());
        metricMetadata.setHidden(metricYaml.getHidden());
        metricMetadata.setExpression(metricYaml.getExpr());
        metricMetadata.setUnit(metricYaml.getUnits());
        return metricMetadata;
    }

    public static DatasetType convertDatasetType(String type){
        if (type!=null){
            if (type.equals("Ramses"))
                return DatasetType.Ramses;
        }
        return DatasetType.Imhotep;
    }
}
