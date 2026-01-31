from custom_types.LUCARIO.type import LUCARIO
from custom_types.PDF.type import PDF
from typing import List
from time import sleep
import logging

class Pipeline:
    def __init__(self,
                lucario_url: str = 'https://lucario.deepdocs.net'
                 ):
        self.lucario_url = lucario_url
    
    def __call__(self,
                 pdfs: List[PDF]) -> LUCARIO:
        file_names = [p.file_name for p in pdfs]
        logging.info(
            "Lucario pipeline input: n_pdfs=%s, file_names=%s",
            len(pdfs), file_names,
        )
        l = LUCARIO.get_new(self.lucario_url)
        for pdf in pdfs:
            l.post_file(pdf.file_as_bytes, pdf.file_name)
        l.update()
        max_wait = max(200, 120 + 45 * len(pdfs))
        l.wait_for_pendings(max_wait=max_wait)
        logging.info(
            "Lucario pipeline output: project_id=%s, n_elements=%s",
            l.project_id, len(l.elements),
        )
        return l