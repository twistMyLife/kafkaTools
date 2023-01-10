import com.cetc10.Application;
import com.cetc10.Mapper.ProducerMapper;
import com.cetc10.domain.po.ProducerInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class SpringTest {

    @Autowired
    ProducerMapper producerMapper;

    @Test
    public void selectAll(){
        List<ProducerInfo> producerInfos = producerMapper.selectList(null);
        System.out.println();
    }
}
