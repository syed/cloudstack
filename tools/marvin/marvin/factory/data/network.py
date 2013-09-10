# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import factory
from marvin.legacy.utils import random_gen
from marvin.factory.network import NetworkFactory
from marvin.factory.data.networkoffering import DefaultIsolatedNetworkOfferingWithSourceNatServiceFactory
from marvin.factory.data.networkoffering import DefaultSharedNetworkOfferingFactory

class GuestIsolatedNetworkFactory(NetworkFactory):

    displaytext = factory.Sequence(lambda n: 'GuestIsolatedNetwork-%s' % random_gen())
    name = factory.Sequence(lambda n: 'GuestIsolatedNetwork-%s' % random_gen())
    networkoffering =\
    factory.SubFactory(
        DefaultIsolatedNetworkOfferingWithSourceNatServiceFactory,
        apiclient=factory.SelfAttribute('..apiclient'),
        name=factory.Sequence(lambda n: 'GuestIsolatedNetworkOffering-%s' % random_gen()),
        enable__apiclient=factory.SelfAttribute('..apiclient')
    )
    networkofferingid = factory.LazyAttribute(lambda no: no.networkoffering.id if no.networkoffering else no.networkoffering)
    zoneid = None

class SharedNetworkFactory(NetworkFactory):

    displaytext = factory.Sequence(lambda n: 'SharedNetwork-%s' % random_gen())
    name = factory.Sequence(lambda n: 'SharedNetwork-%d' % random_gen())
    networkoffering = \
        factory.SubFactory(
            DefaultSharedNetworkOfferingFactory,
            apiclient=factory.SelfAttribute('..apiclient'),
            name=factory.Sequence(lambda n: 'SharedNetworkOffering-%s' % random_gen())
        )
    networkofferingid = factory.LazyAttribute(lambda no: no.networkoffering.id if not no.networkoffering else no.networkoffering)
    zoneid = None