/*! START TRANSACTION */;
CREATE TABLE training_and_experience_categories (
  element_id CHARACTER VARYING(20) NOT NULL,
  scale_id CHARACTER VARYING(3) NOT NULL,
  category DECIMAL(3,0) NOT NULL,
  category_description CHARACTER VARYING(1000) NOT NULL,
  PRIMARY KEY (element_id, scale_id, category),
  FOREIGN KEY (element_id) REFERENCES content_model_reference(element_id),
  FOREIGN KEY (scale_id) REFERENCES scales_reference(scale_id));
/*! COMMIT */;
/*! START TRANSACTION */;

INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 1, 'None');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 2, 'Up to and including 1 month');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 3, 'Over 1 month, up to and including 3 months');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 4, 'Over 3 months, up to and including 6 months');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 5, 'Over 6 months, up to and including 1 year');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 6, 'Over 1 year, up to and including 2 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 7, 'Over 2 years, up to and including 4 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 8, 'Over 4 years, up to and including 6 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 9, 'Over 6 years, up to and including 8 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 10, 'Over 8 years, up to and including 10 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.1', 'RW', 11, 'Over 10 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 1, 'None');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 2, 'Up to and including 1 month');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 3, 'Over 1 month, up to and including 3 months');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 4, 'Over 3 months, up to and including 6 months');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 5, 'Over 6 months, up to and including 1 year');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 6, 'Over 1 year, up to and including 2 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 7, 'Over 2 years, up to and including 4 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 8, 'Over 4 years, up to and including 10 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.2', 'PT', 9, 'Over 10 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 1, 'None or short demonstration');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 2, 'Anything beyond short demonstration, up to and including 1 month');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 3, 'Over 1 month, up to and including 3 months');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 4, 'Over 3 months, up to and including 6 months');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 5, 'Over 6 months, up to and including 1 year');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 6, 'Over 1 year, up to and including 2 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 7, 'Over 2 years, up to and including 4 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 8, 'Over 4 years, up to and including 10 years');
INSERT INTO training_and_experience_categories (element_id, scale_id, category, category_description) VALUES ('3.A.3', 'OJ', 9, 'Over 10 years');
/*! COMMIT */;

