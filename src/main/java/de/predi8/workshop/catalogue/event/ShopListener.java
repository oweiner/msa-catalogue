package de.predi8.workshop.catalogue.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.predi8.workshop.catalogue.domain.Article;
import de.predi8.workshop.catalogue.repository.ArticleRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

@Service
public class ShopListener {
	private final ObjectMapper mapper;
	private final ArticleRepository articleRepository;
	private final NullAwareBeanUtilsBean beanUtils;

	public ShopListener(ObjectMapper mapper, ArticleRepository articleRepository, NullAwareBeanUtilsBean beanUtils) {
		this.mapper = mapper;
		this.articleRepository = articleRepository;
		this.beanUtils = beanUtils;
	}

	@KafkaListener(topics = "shop")
	public void listen(Operation op) throws InvocationTargetException, IllegalAccessException {

		if (!op.getBo().equals("article")) {
			return;
		}

		op.logReceive();

		System.out.println(op);

		Article newArticle = mapper.convertValue(op.getObject(), Article.class);
		if (newArticle == null) {
			return;
		}
		switch (op.getAction()) {

			case "create":
				articleRepository.saveAndFlush(newArticle);
				break;

			case "update":
				Article article = articleRepository.findOne(newArticle.getUuid());
				/*
				if (newArticle.getName() != null) {
					article.setName(newArticle.getName());
				}
				if (newArticle.getPrice() != null) {
					article.setPrice(newArticle.getPrice());
				}
				*/
				beanUtils.copyProperties(article, newArticle);

				articleRepository.saveAndFlush(article);
				break;

			case "delete":
				articleRepository.delete(newArticle.getUuid());
				break;
		}
	}
}