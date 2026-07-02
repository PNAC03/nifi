/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sssc.processors.customlistazureblobstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.azure.storage.AbstractListAzureProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo.Builder;
import org.apache.nifi.processors.azure.storage.utils.BlobServiceClientFactory;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsDetails_v12;
import org.apache.nifi.services.azure.storage.AzureStorageCredentialsService_v12;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.BLOB_STORAGE_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils.getProxyOptions;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBNAME;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_BLOBTYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_CONTAINER;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LANG;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_MIME_TYPE;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_PRIMARY_URI;
import static org.apache.nifi.processors.azure.storage.utils.BlobAttributes.ATTR_NAME_TIMESTAMP;

@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "custom", "azure", "microsoft", "cloud", "storage", "blob" })
@CapabilityDescription("Lists blobs in an Azure Blob Storage container. Listing details are attached to an empty FlowFile for use with FetchAzureBlobStorage. " +
        "The processor uses Azure Blob Storage client library v12.")
@Stateful(scopes = { Scope.CLUSTER }, description = "After performing a listing of blobs, the timestamp of the newest blob is stored if 'Tracking Timestamps' Listing Strategy is in use " +
        "(by default). This allows the Processor to list only blobs that have been added or modified after this date the next time that the Processor is run.")

public class CustomListAzureBlobStorage extends AbstractListAzureProcessor<BlobInfo> {
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles for each listed blob")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A zero-byte FlowFile if no blobs were found")
            .build();

    public static final PropertyDescriptor CONTAINER = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AzureStorageUtils.CONTAINER)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor BLOB_NAME_PREFIX = new PropertyDescriptor.Builder()
            .name("blob-name-prefix")
            .displayName("Blob Name Prefix")
            .description("Search prefix for listing")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();

    public static final PropertyDescriptor TRACKING_STATE_CACHE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_STATE_CACHE)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor TRACKING_TIME_WINDOW = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.TRACKING_TIME_WINDOW)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    public static final PropertyDescriptor INITIAL_LISTING_TARGET = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(ListedEntityTracker.INITIAL_LISTING_TARGET)
            .dependsOn(LISTING_STRATEGY, BY_ENTITIES)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BLOB_STORAGE_CREDENTIALS_SERVICE,
            CONTAINER,
            BLOB_NAME_PREFIX,
            RECORD_WRITER,
            LISTING_STRATEGY,
            TRACKING_STATE_CACHE,
            TRACKING_TIME_WINDOW,
            INITIAL_LISTING_TARGET,
            MIN_AGE,
            MAX_AGE,
            MIN_SIZE,
            MAX_SIZE,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE
    );

    private volatile BlobServiceClientFactory clientFactory;
    
    private volatile Map<String, String> incomingAttributes = new HashMap<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return Collections.unmodifiableSet(relationships);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        clientFactory = new BlobServiceClientFactory(getLogger(), getProxyOptions(context));
    }

    @OnStopped
    public void onStopped() {
        clientFactory = null;
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(CONTAINER).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return BlobInfo.getRecordSchema();
    }

    @Override
    protected Integer countUnfilteredListing(ProcessContext context) throws IOException {
        return null;
    }

    @Override
    protected String getListingContainerName(ProcessContext context) {
        return String.format("Azure Blob Storage Container [%s]", getPath(context));
    }

    @Override
    protected String getDefaultTimePrecision() {
        // User does not have to choose one.
        // AUTO_DETECT can handle most cases, but it may incur longer latency
        // when all listed files do not have SECOND part in their timestamps although Azure Blob Storage does support seconds.
        return PRECISION_SECONDS.getValue();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        return BLOB_STORAGE_CREDENTIALS_SERVICE.equals(property)
                || CONTAINER.equals(property)
                || BLOB_NAME_PREFIX.equals(property)
                || LISTING_STRATEGY.equals(property);
    }

    @Override
    protected List<BlobInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode) throws IOException {
        final BlobServiceClientFactory currentClientFactory;
        if (ListingMode.CONFIGURATION_VERIFICATION == listingMode) {
            currentClientFactory = new BlobServiceClientFactory(getLogger(), getProxyOptions(context));
        } else {
            currentClientFactory = clientFactory;
        }

        final String containerName = context.getProperty(CONTAINER).evaluateAttributeExpressions().getValue();
        final String prefix = context.getProperty(BLOB_NAME_PREFIX).evaluateAttributeExpressions().getValue();
        final long minimumTimestamp = minTimestamp == null ? 0 : minTimestamp;

        try {
            final List<BlobInfo> listing = new ArrayList<>();

            final AzureStorageCredentialsService_v12 credentialsService = context.getProperty(BLOB_STORAGE_CREDENTIALS_SERVICE).asControllerService(AzureStorageCredentialsService_v12.class);
            final AzureStorageCredentialsDetails_v12 credentialsDetails = credentialsService.getCredentialsDetails(Collections.emptyMap());
            final BlobServiceClient storageClient = currentClientFactory.getStorageClient(credentialsDetails);

            final BlobContainerClient containerClient = storageClient.getBlobContainerClient(containerName);

            final ListBlobsOptions options = new ListBlobsOptions()
                    .setPrefix(prefix);

            for (BlobItem blob : containerClient.listBlobs(options, null)) {
                final BlobItemProperties properties = blob.getProperties();

                if (isFileInfoMatchesWithAgeAndSize(context, minimumTimestamp, properties.getLastModified().toInstant().toEpochMilli(), properties.getContentLength())) {
                    final Builder builder = new Builder()
                            .containerName(containerName)
                            .blobName(blob.getName())
                            .primaryUri(String.format("%s/%s", containerClient.getBlobContainerUrl(), blob.getName()))
                            .etag(properties.getETag())
                            .blobType(properties.getBlobType().toString())
                            .contentType(properties.getContentType())
                            .contentLanguage(properties.getContentLanguage())
                            .lastModifiedTime(properties.getLastModified().toInstant().toEpochMilli())
                            .length(properties.getContentLength());

                    listing.add(builder.build());
                }
            }

            return listing;
        } catch (Throwable t) {
            throw new IOException();
        }
    }

    @Override
    protected Map<String, String> createAttributes(BlobInfo entity, ProcessContext context) {
        Map<String, String> attributes = new HashMap<>();
        
        String processorId = getIdentifier();

        attributes.put(ATTR_NAME_CONTAINER, entity.getContainerName());
        attributes.put(ATTR_NAME_BLOBNAME, entity.getBlobName());
        attributes.put(ATTR_NAME_PRIMARY_URI, entity.getPrimaryUri());
        attributes.put(ATTR_NAME_ETAG, entity.getEtag());
        attributes.put(ATTR_NAME_BLOBTYPE, entity.getBlobType());
        attributes.put(ATTR_NAME_MIME_TYPE, entity.getContentType());
        attributes.put(ATTR_NAME_LANG, entity.getContentLanguage());
        attributes.put(ATTR_NAME_TIMESTAMP, String.valueOf(entity.getTimestamp()));
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(entity.getLength()));
        
        attributes.put(CoreAttributes.FILENAME.key(), entity.getName());
        
        attributes.put("run_id", incomingAttributes.get("run_id"));
        attributes.put("master", incomingAttributes.get("master"));
        attributes.put("tenant_id", incomingAttributes.get("tenant_id"));
        attributes.put("bu_id", incomingAttributes.get("bu_id"));
        attributes.put("start_time", incomingAttributes.get("start_time"));
        attributes.put("stage_number", incomingAttributes.get("stage_number"));
        attributes.put("processor_start_time", incomingAttributes.get("processor_start_time"));
        attributes.put("processor_name", "ListAzureBlobStorage");
        attributes.put("processor_uuid", processorId);
        attributes.put("initial_record_count", "0");
        attributes.put("success_record_count", "0");
        attributes.put("failure_record_count", "0");
        attributes.put("source", "Blob Storage");
       
        return attributes;
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        
        String currentStepStr = flowFile.getAttribute("stage_number");
        int currentStep = (currentStepStr != null) ? Integer.parseInt(currentStepStr) : 0;
        
        DateTimeFormatter formatterFull = DateTimeFormatter
        	    .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        	    .withZone(ZoneOffset.UTC);

        String processorStartTime = formatterFull.format(Instant.now());
        
        incomingAttributes.clear(); 
        incomingAttributes.put("run_id", flowFile.getAttribute("run_id"));
        incomingAttributes.put("master", flowFile.getAttribute("master"));
        incomingAttributes.put("tenant_id", flowFile.getAttribute("tenant_id"));
        incomingAttributes.put("bu_id", flowFile.getAttribute("bu_id"));
        incomingAttributes.put("start_time", flowFile.getAttribute("start_time"));
        incomingAttributes.put("stage_number", String.valueOf(currentStep + 1));
        incomingAttributes.put("processor_start_time", processorStartTime);

        session.remove(flowFile);

        super.onTrigger(context, session);
    }
}
