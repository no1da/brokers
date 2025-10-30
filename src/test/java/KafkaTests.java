import org.example.KafkaMessageReceiver;
import org.example.KafkaMessageSender;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

public class KafkaTests {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ORDERS_TOPIC = "orders";
    private static final String PAYMENTS_TOPIC = "payments";
    private static final String USER_ACTIONS_TOPIC = "user_actions";
    private static final int TIMEOUT = 5000;

    private static KafkaMessageSender sender;
    private static KafkaMessageReceiver receiver;

    @BeforeClass
    public static void setUp() {
        sender = new KafkaMessageSender(BOOTSTRAP_SERVERS);
        receiver = new KafkaMessageReceiver(BOOTSTRAP_SERVERS);
    }

    @AfterClass
    public static void tearDown() {
        sender.close();
    }

    @Test
    public void testSendAndReceiveOrders() throws Exception {
        String testMessage = "сообщение 1";
        sender.sendMessage(ORDERS_TOPIC, testMessage);

        List<String> received = receiver.receiveMessages(ORDERS_TOPIC, TIMEOUT);
        Assert.assertFalse(received.isEmpty(), "Должен получить хотя бы 1 сообщение");
        Assert.assertEquals(received.get(received.size() - 1), testMessage);
    }

    @Test
    public void testSendAndReceivePayments() throws Exception {
        String testMessage = "сообщение 2";
        sender.sendMessage(PAYMENTS_TOPIC, testMessage);

        List<String> received = receiver.receiveMessages(PAYMENTS_TOPIC, TIMEOUT);
        Assert.assertFalse(received.isEmpty(), "Должен получить хотя бы 1 сообщение");
        Assert.assertEquals(received.get(received.size() - 1), testMessage);
    }

    @Test
    public void testSendAndReceiveUserActionsTwoGroups() throws Exception {
        String testMessage = "user logout";
        sender.sendMessage(USER_ACTIONS_TOPIC, testMessage);

        List<String> receivedA = receiver.receiveMessages(USER_ACTIONS_TOPIC, TIMEOUT, "groupA");
        Assert.assertFalse(receivedA.isEmpty(), "GroupA должен получить хотя бы 1 сообщение");
        Assert.assertEquals(receivedA.get(receivedA.size() - 1), testMessage);

        List<String> receivedB = receiver.receiveMessages(USER_ACTIONS_TOPIC, TIMEOUT, "groupB");
        Assert.assertFalse(receivedB.isEmpty(), "GroupB должен получить хотя бы 1 сообщение");
        Assert.assertEquals(receivedB.get(receivedB.size() - 1), testMessage);
    }
}