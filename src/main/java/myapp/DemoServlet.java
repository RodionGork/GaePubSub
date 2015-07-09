package myapp;

import java.io.IOException;
import javax.servlet.http.*;
import java.io.InputStream;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.appengine.api.utils.SystemProperty;

public class DemoServlet extends HttpServlet {

    public static String PROJECT_ID = SystemProperty.applicationId.get();

    private static Pubsub pubsub;

    private ObjectMapper mapper = new ObjectMapper();

    private static Map<String, Object> OK_RESPONSE = Collections.singletonMap("ok", (Object) true);

    public Pubsub getPubsub() {
        if (pubsub == null) {
            try {
                pubsub = PubsubConfig.createPubsubClient();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return pubsub;
    }

    private List<String> listTopics() throws IOException {
        ListTopicsResponse resp = getPubsub().projects().topics().list("projects/" + PROJECT_ID).execute();
        if (resp.getTopics() == null) {
            return Collections.emptyList();
        }
        List<String> res = new ArrayList<>();
        for (Topic topic : resp.getTopics()) {
            res.add(topic.getName().replaceFirst(".*\\/topics\\/", ""));
        }
        return res;
    }

    private List<String> listSubscriptions() throws IOException {
        ListSubscriptionsResponse resp = getPubsub().projects().subscriptions().list("projects/" + PROJECT_ID).execute();
        if (resp.getSubscriptions() == null) {
            return Collections.emptyList();
        }
        List<String> res = new ArrayList<>();
        for (Subscription subscription : resp.getSubscriptions()) {
            res.add(subscription.getName().replaceFirst(".*\\/subscriptions\\/", ""));
        }
        return res;
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        Map<String, ?> result;
        String q = req.getParameter("q");
        String name = req.getParameter("name");
        if ("topics".equals(q)) {
            result = Collections.singletonMap("topics", listTopics());
        } else if ("topic".equals(q)) {
            result = createTopic(name);
        } else if ("topic-del".equals(q)) {
            result = deleteTopic(name);
        } else if ("subs".equals(q)) {
            result = Collections.singletonMap("subs", listSubscriptions());
        } else if ("sub".equals(q)) {
            result = createSubscription(name, req.getParameter("for"));
        } else if ("sub-del".equals(q)) {
            result = deleteSubscription(name);
        } else if ("fetch".equals(q)) {
            resp.setContentType("text/plain");
            String data = fetchData(name);
            resp.getWriter().println(data);
            return;
        } else {
            result = Collections.singletonMap("name", "World");
        }
        resp.setContentType("application/json");
        resp.getWriter().println(mapper.writeValueAsString(result));
    }

    private Map<String, Object> createTopic(String name) {
        try {
            getPubsub().projects().topics()
                    .create(fullTopicName(name), new Topic()).execute();
        } catch (Exception e) {
            return errorResponse(e);
        }
        return OK_RESPONSE;
    }

    private Map<String, Object> deleteTopic(String name) {
        try {
            getPubsub().projects().topics().delete(fullTopicName(name)).execute();
        } catch (Exception e) {
            return errorResponse(e);
        }
        return OK_RESPONSE;
    }

    private Map<String, Object> createSubscription(String name, String topic) {
        try {
            Subscription sub = new Subscription().setTopic(fullTopicName(topic)).setAckDeadlineSeconds(15);
            getPubsub().projects().subscriptions().create(fullSubscriptionName(name), sub).execute();
        } catch (Exception e) {
            return errorResponse(e);
        }
        return OK_RESPONSE;
    }

    private Map<String, Object> deleteSubscription(String name) {
        try {
            getPubsub().projects().subscriptions().delete(fullSubscriptionName(name)).execute();
        } catch (Exception e) {
            return errorResponse(e);
        }
        return OK_RESPONSE;
    }

    private String fetchData(String name) {
        name = fullSubscriptionName(name);
        PullRequest req = new PullRequest().setReturnImmediately(false).setMaxMessages(1);
        PullResponse resp;
        try {
            resp = getPubsub().projects().subscriptions().pull(name, req).execute();
        } catch (Exception e) {
            return "Nothing was received... :(";
        }
        List<ReceivedMessage> msgs = resp.getReceivedMessages();
        if (msgs == null || msgs.isEmpty()) {
            return "Empty response for unknown reason... :(";
        }
        AcknowledgeRequest ack = new AcknowledgeRequest().setAckIds(Arrays.asList(msgs.get(0).getAckId()));
        try {
            getPubsub().projects().subscriptions().acknowledge(name, ack).execute();
        } catch (Exception e) {
            return "Error while sending acknowledgement :(";
        }
        return new String(msgs.get(0).getMessage().decodeData());
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        resp.setContentType("text/plain");

        InputStream in = req.getInputStream();
        String data = new Scanner(in).useDelimiter("\\?{5}").next();
        in.close();
        String topic = fullTopicName(req.getParameter("topic"));

        PubsubMessage psmsg = new PubsubMessage();
        psmsg.encodeData(data.getBytes("UTF-8"));
        PublishRequest publishRequest = new PublishRequest().setMessages(Arrays.asList(psmsg));
        PublishResponse publishResponse = getPubsub().projects().topics()
                .publish(topic, publishRequest).execute();
        resp.getWriter().println("Message IDs: " + publishResponse.getMessageIds());
    }

    private String fullTopicName(String name) {
        return "projects/" + PROJECT_ID + "/topics/" + name;
    }

    private String fullSubscriptionName(String name) {
        return "projects/" + PROJECT_ID + "/subscriptions/" + name;
    }

    private Map<String,Object> errorResponse(Exception e) {
        Map<String, Object> map = new HashMap<>();
        map.put("ok", false);
        map.put("error", e.toString());
        map.put("message", e.getMessage());
        return map;
    }

}
