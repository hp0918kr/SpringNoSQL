package kopo.poly.persistance.mongodb.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import kopo.poly.dto.MelonDTO;
import kopo.poly.util.*;
import kopo.poly.persistance.mongodb.AbstractMongoDBcomon;
import kopo.poly.persistance.mongodb.IMelonMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class MelonMapper extends AbstractMongoDBcomon implements IMelonMapper {

    private final MongoTemplate mongodb;

    @Override
    public int insertSong(List<MelonDTO> pList, String colNm) throws Exception {

        log.info(this.getClass().getName() + ".insertSong Start");

        int res = 0;

        if (pList == null) {
            pList = new LinkedList<>();
        }

        // 데이터를 저장할 컬렉션 생성
        super.createCollection(mongodb, colNm, "collectTime");

        // 저장할 컬렉션 객체 생성
        MongoCollection<Document> col = mongodb.getCollection(colNm);

        for (MelonDTO pDTO : pList) {
            // 레코드 한개씩 저장
            col.insertOne(new Document(new ObjectMapper().convertValue(pDTO, Map.class)));
        }

        res = 1;

        log.info(this.getClass().getName() + ".insertSong End");

        return res;
    }

    @Override
    public List<MelonDTO> getSongList(String colNm) throws Exception {

        log.info(this.getClass().getName() + ".getSongList Start");

        // 조회 결과를 전달하기 위한 객체 생성
        List<MelonDTO> rList = new LinkedList<>();

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        // 조회 결과 중 출력할 컬럼들(SQL의 SELECT절과 FROM절 가운데 컬럼들과 유사)
        Document projection = new Document();
        projection.append("song", "$song");
        projection.append("singer", "$singer");

        // mongoDB는 무조건 ObjectId가 자동 생성되며, ObjectId는 사용하지 않을 때, 조회할 필요가 없음
        // ObjectId를 가지고 오지 않을때 사용
        projection.append("_id", 0);

        // MongoDB의 find 명령어를 통해 조회할 경우 사용
        // 조회하는 데이터의 양이 적은 경우, find를 사용, 데이터양이 많은 경우 무조건 Aggregate을 사용
        FindIterable<Document> rs = col.find(new Document()).projection(projection);

        for (Document doc : rs) {
            String song = CmmUtil.nvl(doc.getString("song"));
            String singer = CmmUtil.nvl(doc.getString("singer"));

            log.info("song : " + song + "/ singer : " + singer);

            MelonDTO pDTO = MelonDTO.builder().song(song).singer(singer).build();

            // 레코드 결과를 List에 저장
            rList.add(pDTO);

        }
        log.info(this.getClass().getName() + ".getSongList End");

        return rList;
    }
}