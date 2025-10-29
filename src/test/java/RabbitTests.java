import org.example.Receive;
import org.example.Send;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RabbitTests {
    private static final String QUEUE_NAME = "task_queue";
    private static final String TEST_MESSAGE = "Hello RabbitMQ";
    private Send sender;
    private Receive receiver;

    @BeforeClass
    public void clearQueue() throws Exception {
        sender = new Send();
        receiver = new Receive();
    }

    @Test
    public void testSendAndReceiveMessage() throws Exception {
        sender.sendMessage(TEST_MESSAGE, QUEUE_NAME);
        String received = pollMessageWithTimeout(QUEUE_NAME, 3000);
        Assert.assertNotNull(received, "Сообщение должно быть получено");
        Assert.assertEquals(received, TEST_MESSAGE);
    }

    private String pollMessageWithTimeout(String queue, int timeoutMs) throws Exception {
        long end = System.currentTimeMillis() + timeoutMs;
        do {
            String msg = receiver.receiveMessage(queue);
            if (msg != null) return msg;
            Thread.sleep(50);
        } while(System.currentTimeMillis() < end);
        return null;
    }
}
