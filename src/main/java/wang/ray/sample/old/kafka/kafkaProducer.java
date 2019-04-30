package wang.ray.sample.old.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * 旧版kafka java client producer
 *
 * @author ray
 */
public class kafkaProducer extends Thread {

    private String topic;

    public kafkaProducer(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public void run() {
        Producer producer = createProducer();
        for (int i = 0; i < 1; i++) {
//            String message = "message-" + i;
            // not mpos
//            String message = "{\"LOGDAT\":\"20190311\",\"TACC_DT\":\"0311\",\"CCKFLG\":\"0\",\"TTXNCD\":\"\",\"THDCHK\":\"0\",\"TXN_F13\":\"\",\"RTRCOD\":\"\",\"TXN_F12\":\"\",\"TXN_F11\":\"\",\"TXN_F10\":\"\",\"TXN_STS\":\"F\",\"TXN_F16\":\"\",\"TXN_F15\":\"PI030080856322E332E312E31050E595036323030303033333034333904023034|PI03004023034050E595036323030303033333034333908085631363031303741\",\"END_FLG\":\"0\",\"TXN_F14\":\"\",\"TLOG_NO\":\"\",\"TERMID\":\"43596754\",\"MERTYP\":\"5398\",\"SYNFLG\":\"2\",\"CRDSQN\":\"\",\"TXNTIM\":\"20190311143834\",\"CRD_NO\":\"4895920331954726\",\"PROCOD\":\"000000\",\"MERCID\":\"822331053980199\",\"POSCND\":\"00\",\"CRDFLG\":\"01\",\"CTXNTM\":\"143834\",\"FEE\":\"0\",\"TXN_F5\":\"\",\"TXN_F6\":\"\",\"TLR_ID\":\"\",\"TXN_F7\":\"\",\"TXN_F8\":\"\",\"TXN_F1\":\"S A8V2.3.1.17095.0\",\"ACQRSV\":\"\",\"HACQCD\":\"\",\"AC_NO\":\"4895920331954726\",\"CRDNO1\":\"\",\"TXN_F2\":\"\",\"ACC_DT\":\"20190311\",\"CCYCOD\":\"156\",\"TXN_F3\":\"00000600030000000000001\",\"HLOG_NO\":\"\",\"TXN_F4\":\"\",\"ACTTYP\":\"\",\"REFAMT\":\"0\",\"TXN_F9\":\"\",\"CTXNDT\":\"0311\",\"HSTFIL\":\"0\",\"TXN_TYP\":\"N\",\"EMVFLG\":\"0\",\"FRSP_CD\":\"000055\",\"STXN_CD\":\"012001\",\"OLOG_NO\":\"\",\"RTRSVR\":\"MUNIEFSA\",\"INSADR\":\"浙江兰树化妆品有限公司\",\"MERCTEL\":\"11   11    7971\",\"BATNO\":\"000001\",\"SREFNO\":\"031150975928\",\"TXNAMT\":\"000000004200000\",\"HSTDAT\":\"143833\",\"RSVDAT\":\"01000060\",\"CPSFIL\":\"0\",\"HRSP_CD\":\"\",\"SCPSCOD\":\"55\",\"ICLEN\":\"0\",\"TRMTYP\":\"H\",\"LOG_NO\":\"19031150975928\",\"TXNRSV3\":\"\",\"HFORCD\":\"\",\"ICDAT\":\"\",\"TXNRSV2\":\"\",\"BR_NO\":\"999999\",\"ACQCOD\":\"48223310\",\"TXNRSV1\":\"\",\"ISSINO\":\"01050001\",\"CPSRSV\":\"\",\"TTXN_STS\":\"F\",\"ORNDAT\":\"0200\",\"HTXN_STS\":\"U\",\"TMSTIM\":\"0311143834\",\"TRSP_CD\":\"000055\",\"HISINO\":\"\",\"IDNO\":\"0|00\",\"BILLNO\":\"000195\",\"MSGID\":\"0200\",\"CSEQNO\":\"000195\",\"CPSDAT\":\"\",\"AUTCOD\":\"\",\"AGTORG\":\"CUPS\",\"FORCOD\":\"48220000\",\"NOD_NO\":\"COR5\",\"INMOD\":\"021\"}";
            // mpos
//            String message = "{\"LOGDAT\":\"20190311\",\"TACC_DT\":\"0311\",\"CCKFLG\":\"0\",\"TTXNCD\":\"\",\"THDCHK\":\"0\",\"TXN_F13\":\"\",\"RTRCOD\":\"\",\"TXN_F12\":\"\",\"TXN_F11\":\"\",\"TXN_F10\":\"\",\"TXN_STS\":\"S\",\"TXN_F16\":\"\",\"TXN_F15\":\"|PI02604023033050A3033443030393138343408085631363031303741\",\"END_FLG\":\"0\",\"TXN_F14\":\"\",\"TLOG_NO\":\"\",\"TERMID\":\"41550560\",\"MERTYP\":\"7011\",\"SYNFLG\":\"2\",\"CRDSQN\":\"000\",\"TXNTIM\":\"20190311143835\",\"CRD_NO\":\"6259760020065557\",\"PROCOD\":\"000000\",\"MERCID\":\"822711159942935\",\"POSCND\":\"00\",\"CRDFLG\":\"01\",\"CTXNTM\":\"143835\",\"FEE\":\"0\",\"TXN_F5\":\"2FF7354F655D4E3FBBB16E7EDA6E5DF0\",\"TXN_F6\":\"\",\"TLR_ID\":\"\",\"TXN_F7\":\"\",\"TXN_F8\":\"\",\"TXN_F1\":\"\",\"ACQRSV\":\"\",\"HACQCD\":\"\",\"AC_NO\":\"6259760020065557\",\"CRDNO1\":\"\",\"TXN_F2\":\"\",\"ACC_DT\":\"20190311\",\"CCYCOD\":\"156\",\"TXN_F3\":\"00000500030000000000001\",\"HLOG_NO\":\"\",\"TXN_F4\":\"00FF\",\"ACTTYP\":\"\",\"REFAMT\":\"0\",\"TXN_F9\":\"\",\"CTXNDT\":\"0311\",\"HSTFIL\":\"0\",\"TXN_TYP\":\"N\",\"EMVFLG\":\"1\",\"FRSP_CD\":\"000000\",\"STXN_CD\":\"012001\",\"OLOG_NO\":\"\",\"RTRSVR\":\"MUNIEFSA\",\"INSADR\":\"成都小围服装有限公司\",\"MERCTEL\":\"\",\"BATNO\":\"000003\",\"SREFNO\":\"031110607092\",\"TXNAMT\":\"000000000200000\",\"HSTDAT\":\"143834\",\"RSVDAT\":\"01000050\",\"CPSFIL\":\"0\",\"HRSP_CD\":\"\",\"SCPSCOD\":\"00\",\"ICLEN\":\"0\",\"TRMTYP\":\"6\",\"LOG_NO\":\"19031110607092\",\"TXNRSV3\":\"97326396\",\"HFORCD\":\"\",\"ICDAT\":\"9F26081137D70A9A6D13919F2701809F101307070103A0A802010A0100000000004EFB34A99F37047A2E3FB59F3602003A9505008004E8009A031903119C01009F02060000002000005F2A02015682027C009F1A0201569F03060000000000009F33036040C89F34034203009F3501229F1E08424D5354373830308408A0000003330101029F090200009F410400000000\",\"TXNRSV2\":\"\",\"BR_NO\":\"999999\",\"ACQCOD\":\"48226510\",\"TXNRSV1\":\"822651070112520\",\"ISSINO\":\"63030000\",\"CPSRSV\":\"\",\"TTXN_STS\":\"S\",\"ORNDAT\":\"0200\",\"HTXN_STS\":\"U\",\"TMSTIM\":\"0311143835\",\"TRSP_CD\":\"000000\",\"HISINO\":\"\",\"IDNO\":\"3|00\",\"BILLNO\":\"046648\",\"MSGID\":\"0200\",\"CSEQNO\":\"046648\",\"CPSDAT\":\"\",\"AUTCOD\":\"\",\"AGTORG\":\"CUPS888k\",\"FORCOD\":\"48220000\",\"NOD_NO\":\"K_N20\",\"INMOD\":\"051\"}";
            // activity
            String message = "{\"activityId\":\"1000000001\",\"bizLogId\":\"80000000020190429170305937\",\"desc\":\"签到奖励10分\",\"memberId\":\"1234567890123456@@##\",\"method\":\"lama.acitivity.mposmember.pointBalance\",\"point\":\"+10\",\"requestId\":\"20190429170303911759\",\"requestTime\":\"20190429170303\",\"systemCode\":\"NEW_MPOS_FRONT\",\"txnDate\":\"20190429170303\",\"txnType\":\"002\",\"version\":\"1.0\"}";
            producer.send(new KeyedMessage<>(topic, i + "", message));
            System.out.println("producer message: " + message);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private Producer createProducer() {
        Properties properties = new Properties();
//        properties.put("metadata.broker.list", "10.177.84.73:9092");
        properties.put("metadata.broker.list", "10.177.84.76:9092");
        properties.put("serializer.class", StringEncoder.class.getName());
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public static void main(String[] args) {
//        new kafkaProducer("lkl_zf-lamm-dc_events").start();
        new kafkaProducer("lkl_zf-lama-mposmember_events").start();
    }
}
