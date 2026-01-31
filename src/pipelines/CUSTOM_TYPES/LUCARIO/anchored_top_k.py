from typing import List
import logging
import openai, os, numpy as np, math
from custom_types.JSONL.type import JSONL
from custom_types.LUCARIO.type import LUCARIO

class Pipeline:
    def __init__(self,
                embedding_model : str = 'text-embedding-3-large',
                anchor_key : str = 'anchors',
                max_groups_per_element : int = 1,
                elements_per_group : int = 1,
                min_elements_per_list : int = 1,
                **kwargs : dict
                ):
        self.__dict__.update(locals())
        self.__dict__.pop("self")

    def __call__(self,
                sections: JSONL,
                lucario: LUCARIO,
                ) -> JSONL:
        queries = [_ for section in sections.lines for _ in section[self.anchor_key]]
        logging.info("Lucario anchored_top_k: project_id=%s, n_queries=%s", lucario.project_id, len(queries))
        lucario.update()
        references = {_.file_id: _.description for _ in lucario.elements.values()}
        lines = lucario.anchored_top_k(
            queries=queries,
            group_ids = [i for i, section in enumerate(sections.lines) for _ in section[self.anchor_key]],
            max_groups_per_element = self.max_groups_per_element,
            elements_per_group = self.elements_per_group,
            min_elements_per_list = self.min_elements_per_list
        )
        # enrich the lines with the references
        for line in lines:
            line['reference'] = references.get(line['parent_file_id'], '')
        logging.info("Lucario anchored_top_k done: project_id=%s, n_lines=%s", lucario.project_id, len(lines))
        return JSONL(lines)