package net.opentsdb.odata;

import java.util.ArrayList;
import java.util.List;

import org.odata4j.edm.EdmAssociation;
import org.odata4j.edm.EdmAssociationSet;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.edm.EdmDecorator;
import org.odata4j.edm.EdmEntityContainer;
import org.odata4j.edm.EdmEntitySet;
import org.odata4j.edm.EdmEntityType;
import org.odata4j.edm.EdmGenerator;
import org.odata4j.edm.EdmDataServices.Builder;
import org.odata4j.edm.EdmProperty;
import org.odata4j.edm.EdmSchema;
import org.odata4j.edm.EdmSimpleType;
import org.odata4j.edm.EdmType;

public class OpenTSDBEdmGenerator implements EdmGenerator {

	@Override
	public Builder generateEdm(EdmDecorator decorator) {
		final String namespace = "OpenTSDB";
		
		List<EdmSchema.Builder> schemas = new ArrayList<EdmSchema.Builder>();
		List<EdmEntityContainer.Builder> containers = new ArrayList<EdmEntityContainer.Builder>();
		List<EdmEntitySet.Builder> entitySets = new ArrayList<EdmEntitySet.Builder>();
		List<EdmEntityType.Builder> entityTypes = new ArrayList<EdmEntityType.Builder>();
		List<EdmAssociation.Builder> associations = new ArrayList<EdmAssociation.Builder>();
		List<EdmAssociationSet.Builder> associationSets = new ArrayList<EdmAssociationSet.Builder>();

		/**
		 * The type definition for MetricList
		 */
		EdmProperty.Builder metricNameProperty = EdmProperty.newBuilder("Name")
				.setName("Name")
				.setType(EdmSimpleType.STRING);
		EdmEntityType.Builder metricsType = EdmEntityType.newBuilder()
				.setName("Metric")
				.setNamespace(namespace)
				.setAlias("Alias")
				.addKeys(new String[] {"Name"})
				.addProperties(metricNameProperty);
		EdmEntitySet.Builder metricsSet = EdmEntitySet.newBuilder()
				.setName("MetricList")
				.setEntityType(metricsType);
		entityTypes.add(metricsType);
		entitySets.add(metricsSet);
		
		/**
		 * The type definition for TimeSeries
		 */
		EdmProperty.Builder timeSeriesNameProperty = EdmProperty.newBuilder("Name")
				.setName("Name")
				.setType(EdmSimpleType.STRING);
		EdmProperty.Builder timeSeriesTimestampProperty = EdmProperty.newBuilder("Timestamp")
				.setName("Timestamp")
				.setType(EdmSimpleType.DATETIME);
		EdmProperty.Builder timeSeriesValueProperty = EdmProperty.newBuilder("Value")
				.setName("Value")
				.setType(EdmSimpleType.DOUBLE);
		
		EdmProperty.Builder[] properties = new EdmProperty.Builder[19];
		properties[0] = timeSeriesNameProperty;
		properties[1] = timeSeriesTimestampProperty;
		properties[2] = timeSeriesValueProperty;
		
		for (int i=0; i<16; i++)
		{
			String name = "tag"+(i+1);
			EdmProperty.Builder tag = EdmProperty.newBuilder(name)
					.setName(name)
					.setType(EdmSimpleType.STRING);
			
			properties[i+3]=tag;
		}
		
		EdmEntityType.Builder timeSeriesType = EdmEntityType.newBuilder()
				.setName("TimeSeries")
				.setNamespace(namespace)
				.addKeys(new String[] {"Name", "Timestamp"})
				.addProperties(properties);
		
		EdmEntitySet.Builder timeSeriesSet = EdmEntitySet.newBuilder()
				.setName("TimeSeries")
				.setEntityType(timeSeriesType);
		
		entityTypes.add(timeSeriesType);
		entitySets.add(timeSeriesSet);
		
		EdmEntityContainer.Builder container = EdmEntityContainer.newBuilder()
				.setName(namespace + "Entities")
				.setIsDefault(true)
				.addEntitySets(entitySets)
				.addAssociationSets(associationSets);
		containers.add(container);
		
		EdmSchema.Builder schema = EdmSchema.newBuilder()
				.setNamespace(namespace)
				.addEntityTypes(entityTypes)
				.addAssociations(associations)
				.addEntityContainers(containers);
		schemas.add(schema);
		
		EdmDataServices.Builder rt = EdmDataServices.newBuilder()
				.addSchemas(schemas);
		
		return rt;
	}
}
