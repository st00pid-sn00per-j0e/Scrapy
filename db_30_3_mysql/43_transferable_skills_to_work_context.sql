/*! START TRANSACTION */;
CREATE TABLE transferable_skills_to_work_context (
  transferable_skills_element_id CHARACTER VARYING(20) NOT NULL,
  work_context_element_id CHARACTER VARYING(20) NOT NULL,
  FOREIGN KEY (transferable_skills_element_id) REFERENCES content_model_reference(element_id),
  FOREIGN KEY (work_context_element_id) REFERENCES content_model_reference(element_id));
/*! COMMIT */;
/*! START TRANSACTION */;

INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.a.2.c');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.a.2.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.a.2.l');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.a.4');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.b.1.e');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.b.1.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.d.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.d.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.1.d.3');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.2.a.3');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.a', '4.C.3.c.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.a.2.l');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.b.1.e');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.b.1.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.c.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.d.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.d.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.1.d.3');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.b', '4.C.3.c.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.1.a.2.l');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.1.b.1.e');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.1.b.1.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.1.d.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.1.d.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.1.d.3');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.c', '4.C.3.c.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.d', '4.C.1.b.1.e');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.d', '4.C.1.b.1.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.d', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.d', '4.C.1.d.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.d', '4.C.1.d.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.d', '4.C.1.d.3');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.d', '4.C.3.c.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.e', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.e', '4.C.1.c.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.f', '4.C.1.b.1.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.f', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.1.f', '4.C.1.d.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.2.i', '4.C.3.a.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.2.i', '4.C.3.a.2.a');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.2.i', '4.C.3.a.4');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.3.g', '4.C.2.a.1.e');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.3.g', '4.C.2.a.1.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.3.h', '4.C.2.a.1.e');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.3.h', '4.C.2.a.1.f');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.4.e', '4.C.3.a.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.4.e', '4.C.3.a.2.a');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.4.e', '4.C.3.a.4');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.5.a', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.5.a', '4.C.1.c.2');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.5.a', '4.C.3.b.8');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.5.a', '4.C.3.d.1');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.5.d', '4.C.1.b.1.g');
INSERT INTO transferable_skills_to_work_context (transferable_skills_element_id, work_context_element_id) VALUES ('2.B.5.d', '4.C.1.c.2');
/*! COMMIT */;

