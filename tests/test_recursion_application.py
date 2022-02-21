import datetime
import os
from functools import singledispatchmethod
from typing import Optional, Any, Dict, List, cast
from typing import Type
from uuid import uuid5, NAMESPACE_URL, UUID

from aenum import EnumConstants
from dateutil.tz.tz import tzutc
from eventsourcing.application import AggregateNotFound
from eventsourcing.application import Application, ProcessingEvent
from eventsourcing.domain import Aggregate, event
from eventsourcing.persistence import Recording

os.environ["PERSISTENCE_MODULE"] = "eventsourcing_sqlalchemy"
os.environ["SQLALCHEMY_URL"] = 'mysql://root:@localhost:3306/cmdb'


class K8sNamespace(Aggregate):
    StatusChanged: Type[Aggregate.Event["K8sNamespace"]]

    class Status(EnumConstants):
        NA = 'NA'
        AVAILABLE = 'AVAILABLE'
        WILL_EXPIRE = 'WILL_EXPIRE'
        EXPIRED = 'EXPIRED'
        DELETED = 'DELETED'
        K8S_DELETED = 'K8S_DELETED'

    @event("Registered")
    def __init__(self, k8s_cluster_id: str, namespace: str, creation_timestamp: datetime.datetime,
                 status=Status.NA, owners: Optional[list[str]] = None):
        self.k8s_cluster_id = str(k8s_cluster_id)
        self.namespace = namespace
        self.creation_timestamp = creation_timestamp
        self.status = status
        if owners is None:
            owners = []
        self.owners = owners

    @staticmethod
    def create_id(k8s_cluster_id: str, namespace: str):
        k8s_cluster_id = str(k8s_cluster_id)
        return uuid5(NAMESPACE_URL, f'/k8s_namespace/{k8s_cluster_id}/{namespace}')

    @event("StatusChanged")
    def set_status(self, status: str) -> None:
        self.status = status

    @event("OwnersChanged")
    def set_owners(self, owners: List[str]):
        self.owners = owners

    def to_dict(self):
        data = {"k8s_cluster_id": self.k8s_cluster_id,
                "namespace": self.namespace,
                "creation_timestamp": self.creation_timestamp,
                "status": self.status,
                "owners": self.owners,
                }
        return data


class RecursionEvtApplication(Application):
    def _record(self, processing_event: ProcessingEvent) -> List[Recording]:
        event_counter = 0
        while event_counter < len(processing_event.events):
            new_event = processing_event.events[event_counter]
            self.policy(new_event, processing_event)
            event_counter += 1
        return super()._record(processing_event)

    def _get_aggregate_within_policy(
            self, processing_event: ProcessingEvent, aggregate_id: UUID
    ) -> Aggregate:
        return processing_event.aggregates.get(aggregate_id) or self.repository.get(
            aggregate_id
        )

    def policy(self, domain_event, processing_event) -> None:
        """Default policy"""
        pass


class EvtSourcingApp(RecursionEvtApplication):
    is_snapshotting_enabled = True
    snapshotting_intervals = {
        K8sNamespace: 1
    }

    def register_k8s_namespace(self, k8s_cluster_id, namespace, creation_timestamp,
                               owners: Optional[list[str]] = None) -> UUID:
        if owners is None:
            owners = []
        k8s_namespace = K8sNamespace(k8s_cluster_id=k8s_cluster_id, namespace=namespace, owners=owners,
                                     creation_timestamp=creation_timestamp)
        self.save(k8s_namespace)
        return k8s_namespace.id

    def set_k8s_namespace_status(self, k8s_namespace_id: UUID, status: str) -> None:
        k8s_namespace = cast(K8sNamespace, self.repository.get(k8s_namespace_id))
        k8s_namespace.set_status(status)
        self.save(k8s_namespace)

    def set_k8s_namespace_owners(self, k8s_namespace_id: UUID, owners: List[str]) -> None:
        k8s_namespace = cast(K8sNamespace, self.repository.get(k8s_namespace_id))
        k8s_namespace.set_owners(owners)
        self.save(k8s_namespace)

    def get_k8s_namespace_data(self, k8s_namespace_id: UUID) -> Dict[str, Any]:
        k8s_namespace = cast(K8sNamespace, self.repository.get(k8s_namespace_id))
        return k8s_namespace.to_dict()

    @singledispatchmethod
    def policy(self, domain_event, processing_event) -> None:
        """Default policy"""
        aggregate = self._get_aggregate_within_policy(
            processing_event, domain_event.originator_id
        )
        print(f"Default policy for {aggregate.__class__.__name__} {domain_event.__class__.__name__}")

    @policy.register(K8sNamespace.StatusChanged)
    def _(self, domain_event, processing_event):
        k8s_namespace = cast(
            K8sNamespace,
            self._get_aggregate_within_policy(
                processing_event, domain_event.originator_id
            ),
        )
        if k8s_namespace.status == K8sNamespace.Status.DELETED:
            k8s_namespace.set_status(K8sNamespace.Status.K8S_DELETED)
            # do some remote deletion on k8s
            print(f'Namespace {k8s_namespace.namespace} deleted on k8s cluster {k8s_namespace.k8s_cluster_id}')
            processing_event.collect_events(k8s_namespace)


def test_recursion_app():
    app = EvtSourcingApp()

    # an ID for k8s cluster
    k8s_cluster_id = '1'

    namespace_details_with_owners = [
        {'namespace': 'kube-system', 'creation_timestamp': datetime.datetime(2021, 5, 26, 3, 8, 15, tzinfo=tzutc()),
         'owners': []},
        {'namespace': 'albert1', 'creation_timestamp': datetime.datetime(2021, 12, 29, 6, 37, 41, tzinfo=tzutc()),
         'owners': ['albert']},
    ]

    # Create namespace, and set all as AVAILABLE
    for ns_data in namespace_details_with_owners:
        namespace = ns_data['namespace']
        creation_timestamp = ns_data['creation_timestamp']
        owners = ns_data['owners']
        k8s_namespace_id: UUID = K8sNamespace.create_id(k8s_cluster_id, namespace)
        try:
            k8s_namespace_data = app.get_k8s_namespace_data(k8s_namespace_id)
        except AggregateNotFound as e:
            k8s_namespace_id = app.register_k8s_namespace(k8s_cluster_id=k8s_cluster_id,
                                                          namespace=namespace, creation_timestamp=creation_timestamp,
                                                          owners=owners)
            k8s_namespace_data = app.get_k8s_namespace_data(k8s_namespace_id)

        if k8s_namespace_data["status"] != K8sNamespace.Status.AVAILABLE:
            app.set_k8s_namespace_status(k8s_namespace_id, K8sNamespace.Status.AVAILABLE)

        owners = k8s_namespace_data["owners"]
        if "limenglong" not in owners:
            app.set_k8s_namespace_owners(k8s_namespace_id, owners + ["limenglong"])

    # Deleted the namespace albert1 if exists with status not deleted
    namespace_to_delete = 'albert1'
    k8s_namespace_id = K8sNamespace.create_id(k8s_cluster_id, namespace_to_delete)
    try:
        k8s_namespace_data = app.get_k8s_namespace_data(k8s_namespace_id)
        if k8s_namespace_data['status'] not in {K8sNamespace.Status.DELETED, K8sNamespace.Status.K8S_DELETED}:
            app.set_k8s_namespace_status(k8s_namespace_id, K8sNamespace.Status.DELETED)
    except AggregateNotFound as e:
        pass


if __name__ == '__main__':
    test_recursion_app()
