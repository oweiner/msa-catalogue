package de.predi8.workshop.catalogue.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.predi8.workshop.catalogue.domain.Article;
import de.predi8.workshop.catalogue.error.NotFoundException;
import de.predi8.workshop.catalogue.event.Operation;
import de.predi8.workshop.catalogue.repository.ArticleRepository;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/articles")
public class CatalogueRestController {
	private ArticleRepository articleRepository;
	private KafkaTemplate<String, Operation> kafka;
	private ObjectMapper mapper;

	private Counter counterUpdate;

	public CatalogueRestController(ArticleRepository articleRepository,
								   KafkaTemplate<String, Operation> kafka,
								   ObjectMapper mapper) {
		this.articleRepository = articleRepository;
		this.kafka = kafka;
		this.mapper = mapper;
		this.counterUpdate = Counter.build().name("articleUpdate").help("counter articleUpdate").register();
	}

	@GetMapping
	public List<Article> index() {
		return articleRepository.findAll();
	}

	@GetMapping("/{id}")
	public Article index(@PathVariable String id) {
		Article article = articleRepository.findOne(id);

		if (article == null) {
			throw new NotFoundException();
		}

		return article;
	}

	@PostMapping
	public ResponseEntity create(@RequestBody Article article, UriComponentsBuilder builder)
			throws URISyntaxException, InterruptedException, ExecutionException, TimeoutException {

		System.out.println("article = " + article);

		article.setUuid(UUID.randomUUID().toString());

		Operation op = new Operation("article", "create", mapper.valueToTree(article));

		kafka.send(new ProducerRecord<>("shop", article.getUuid(), op)).get(100, TimeUnit.MILLISECONDS);

		//articleRepository.saveAndFlush(article);

		return ResponseEntity.created(builder.path("/articles/" + article.getUuid()).build().toUri()).build();
	}

	@PatchMapping("/{uuid}")
	public void update(@PathVariable String uuid, @RequestBody Article neu) throws Exception {

		Article article = articleRepository.findOne(uuid);

		if (article == null) {
			throw new NotFoundException();
		}

		Article updateBo = new Article();
		updateBo.setPrice(neu.getPrice());
		updateBo.setName(neu.getName());
		updateBo.setUuid(uuid);

		Operation op = new Operation("article", "update", mapper.valueToTree(updateBo));

		op.logSend();

		kafka.send("shop", op).get(100, TimeUnit.MILLISECONDS);

		counterUpdate.inc();
	}

	@DeleteMapping("/{uuid}")
	public void delete(@PathVariable String uuid) throws Exception {

		Article article = articleRepository.findOne(uuid);

		if (article == null) {
			throw new NotFoundException();
		}

		Operation op = new Operation("article", "delete", mapper.valueToTree(article));

		op.logSend();

		kafka.send("shop", op).get(100, TimeUnit.MILLISECONDS);
	}


	@GetMapping("/count")
	public long count() {
		return articleRepository.count();
	}
}