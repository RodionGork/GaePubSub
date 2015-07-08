package myapp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.common.base.Preconditions;

import java.io.IOException;

public class PubsubConfig {

    public static Pubsub createPubsubClient() throws IOException {
        return createPubsubClient(Utils.getDefaultTransport(),
                Utils.getDefaultJsonFactory());
    }

    public static Pubsub createPubsubClient(HttpTransport httpTransport,
            JsonFactory jsonFactory) throws IOException {
        Preconditions.checkNotNull(httpTransport);
        Preconditions.checkNotNull(jsonFactory);
        GoogleCredential credential = GoogleCredential.getApplicationDefault(
                httpTransport, jsonFactory);
        if (credential.createScopedRequired()) {
            credential = credential.createScoped(PubsubScopes.all());
        }
        HttpRequestInitializer initializer = credential;
        return new Pubsub.Builder(httpTransport, jsonFactory, initializer)
                .setApplicationName("PubSubTest").build();
    }
    
}

