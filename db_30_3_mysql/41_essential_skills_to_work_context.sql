/*! START TRANSACTION */;
CREATE TABLE essential_skills_to_work_context (
  essential_skills_element_id CHARACTER VARYING(20) NOT NULL,
  work_context_element_id CHARACTER VARYING(20) NOT NULL,
  FOREIGN KEY (essential_skills_element_id) REFERENCES content_model_reference(element_id),
  FOREIGN KEY (work_context_element_id) REFERENCES content_model_reference(element_id));
/*! COMMIT */;
/*! START TRANSACTION */;

INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.a', '4.C.1.a.2.h');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.a.2.c');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.a.2.f');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.a.2.l');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.a.4');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.b.1.e');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.b.1.f');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.b.1.g');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.d.1');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.b', '4.C.1.d.2');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.c', '4.C.1.a.2.h');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.c', '4.C.1.a.2.j');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.a.2.c');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.a.2.f');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.a.2.l');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.a.4');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.b.1.e');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.b.1.f');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.b.1.g');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.d.1');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.d.2');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.1.d', '4.C.1.d.3');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.1.b.1.e');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.1.b.1.f');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.1.d.1');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.1.d.3');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.3.a.1');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.3.a.2.a');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.3.a.4');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.3.b.8');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.a', '4.C.3.c.1');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.b', '4.C.3.a.4');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.d', '4.C.1.b.1.e');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.d', '4.C.1.b.1.g');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.d', '4.C.1.c.1');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.d', '4.C.1.c.2');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.d', '4.C.3.b.4');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.d', '4.C.3.b.7');
INSERT INTO essential_skills_to_work_context (essential_skills_element_id, work_context_element_id) VALUES ('2.A.2.d', '4.C.3.d.3');
/*! COMMIT */;

