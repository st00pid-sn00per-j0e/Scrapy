/*! START TRANSACTION */;
CREATE TABLE education_categories (
  element_id CHARACTER VARYING(20) NOT NULL,
  scale_id CHARACTER VARYING(3) NOT NULL,
  category DECIMAL(3,0) NOT NULL,
  category_description CHARACTER VARYING(1000) NOT NULL,
  PRIMARY KEY (element_id, scale_id, category),
  FOREIGN KEY (element_id) REFERENCES content_model_reference(element_id),
  FOREIGN KEY (scale_id) REFERENCES scales_reference(scale_id));
/*! COMMIT */;
/*! START TRANSACTION */;

INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 1, 'Less than a High School Diploma');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 2, 'High School Diploma - or the equivalent (for example, GED)');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 3, 'Post-Secondary Certificate - awarded for training completed after high school (for example, in agriculture or natural resources, computer services, personal or culinary services, engineering technologies, healthcare, construction trades, mechanic and repair technologies, or precision production)');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 4, 'Some College Courses');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 5, 'Associate''s Degree (or other 2-year degree)');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 6, 'Bachelor''s Degree');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 7, 'Post-Baccalaureate Certificate - awarded for completion of an organized program of study; designed for people who have completed a Baccalaureate degree but do not meet the requirements of academic degrees carrying the title of Master.');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 8, 'Master''s Degree');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 9, 'Post-Master''s Certificate - awarded for completion of an organized program of study; designed for people who have completed a Master''s degree but do not meet the requirements of academic degrees at the doctoral level.');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 10, 'First Professional Degree - awarded for completion of a program that: requires at least 2 years of college work before entrance into the program, includes a total of at least 6 academic years of work to complete, and provides all remaining academic requirements to begin practice in a profession.');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 11, 'Doctoral Degree');
INSERT INTO education_categories (element_id, scale_id, category, category_description) VALUES ('2.D.1', 'RL', 12, 'Post-Doctoral Training');
/*! COMMIT */;

