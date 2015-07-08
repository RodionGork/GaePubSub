package myapp;

import java.io.IOException;
import javax.servlet.http.*;
import java.io.InputStream;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Topic;
import com.google.appengine.api.utils.SystemProperty;

public class DemoServlet extends HttpServlet {

    public static String PROJECT_ID = SystemProperty.applicationId.get();

    private static Pubsub pubsub;

    private ObjectMapper mapper = new ObjectMapper();

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
    
    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        resp.setContentType("application/json");
        Map<String, ?> result;
        if ("topics".equals(req.getParameter("q"))) {
            result = Collections.singletonMap("topics", listTopics());
        } else if ("topic".equals(req.getParameter("q"))) {
            result = Collections.singletonMap("ok", createTopic(req.getParameter("name")));
        } else if ("topic-del".equals(req.getParameter("q"))) {
            result = Collections.singletonMap("ok", deleteTopic(req.getParameter("name")));
        } else {
            result = Collections.singletonMap("name", "World");
        }
        resp.getWriter().println(mapper.writeValueAsString(result));
    }

    private boolean deleteTopic(String name) {
        try {
            getPubsub().projects().topics()
                .delete(fullTopicName(name)).execute();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private boolean createTopic(String name) {
        try {
            getPubsub().projects().topics()
                    .create(fullTopicName(name), new Topic()).execute();
        } catch (Exception e) {
            return false;
        }
        return true;
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
}
