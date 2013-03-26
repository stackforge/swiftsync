import swiftclient
import eventlet

from objects import sync_object
from utils import get_config


class Containers(object):
    """Containers syncornization."""
    def __init__(self):
        self.max_gthreads = int(get_config("sync", "max_gthreads"))

    def sync(self, orig_storage_cnx, orig_storage_url,
             orig_token, dest_storage_cnx, dest_storage_url, dest_token,
             container_name):
        orig_container_stats, orig_objects = swiftclient.get_container(
            None, orig_token, container_name, http_conn=orig_storage_cnx,
        )

        try:
            swiftclient.head_container(
                "", dest_token, container_name, http_conn=dest_storage_cnx
            )
        except(swiftclient.client.ClientException):
            container_headers = orig_container_stats.copy()
            for h in ('x-container-object-count', 'x-trans-id',
                      'x-container-bytes-used'):
                del container_headers[h]
            p = dest_storage_cnx[0]
            url = "%s://%s%s" % (p.scheme, p.netloc, p.path)
            swiftclient.put_container(url,
                dest_token, container_name,
                headers=container_headers)

        dest_container_stats, dest_objects = swiftclient.get_container(
            None, dest_token, container_name, http_conn=dest_storage_cnx,
        )

        set1 = set((x['last_modified'], x['name']) for x in orig_objects)
        set2 = set((x['last_modified'], x['name']) for x in dest_objects)

        diff = set1 - set2
        if not diff:
            return

        pool = eventlet.GreenPool(size=self.max_gthreads)
        pile = eventlet.GreenPile(pool)
        for obj in diff:
            print obj
            pile.spawn(sync_object,
                       orig_storage_url,
                       orig_token,
                       dest_storage_url,
                       dest_token, container_name,
                       obj)
        pool.waitall()
