// SWEAT440 Dashboard — User Access Control
// ─────────────────────────────────────────────────────────────────────────────
// Usernames are email addresses (case-insensitive at login).
// To change a password:
//   1. Open generate-hash.html in your browser
//   2. Type the new password → click Generate Hash → copy the hash
//   3. Paste it as the passwordHash below, then push to GitHub
//
// Studio names must match data.json exactly (no "SWEAT440 " prefix).
// Set studios: null for full access to all studios.
// ─────────────────────────────────────────────────────────────────────────────

const USERS = {

  // ── Admin (LeadTeam) — full access ────────────────────────────────────────
  'santiago.estrada@leadteam.com': {
    passwordHash: '32620a96dfe2cf41f26c5751dd0197ff0235421dec2bd8b3f503f187a2665c76',
    name: 'Santiago Estrada',
    studios: null,
    role: 'admin'
  },
  'santiago.estrada@sweat440.com': {
    passwordHash: '32620a96dfe2cf41f26c5751dd0197ff0235421dec2bd8b3f503f187a2665c76',
    name: 'Santiago Estrada',
    studios: null,
    role: 'admin'
  },
  'jamie.westall@leadteam.com': {
    passwordHash: 'd51efb736aca536431204927a0dc30e4909d49b809733b7ac6b1227610b6e679',
    name: 'Jamie Westall',
    studios: null,
    role: 'admin'
  },
  'sebastian.arango@leadteam.com': {
    passwordHash: '82251cee9a5262e36d13362d1b86e38fb0fb3314570ace33171d7aa3f08c33de',
    name: 'Sebastian Arango',
    studios: null,
    role: 'admin'
  },
  'daniel.jimenez@leadteam.com': {
    passwordHash: '24851852c95b5d223f697bb0a9796f93ffb7be334b3951cef647dadd61b33729',
    name: 'Daniel Jimenez',
    studios: null,
    role: 'admin'
  },
  'andre@studioseven.agency': {
    passwordHash: 'c61231d836ec478aed49c3f25a2c9b7b488bae81aabfa53020cceceb4d6e4bd3',
    name: 'Andre T',
    studios: null,
    role: 'admin'
  },
  'laura.londono@leadteam.com': {
    passwordHash: '15743531a3962a0aff9eb020754e49cb58ef8ab6ed81ddfcb674572e60c62f8a',
    name: 'Laura Londoño',
    studios: null,
    role: 'admin'
  },
  'david.diaz@leadteam.com': {
    passwordHash: '582bda4d19e247bdc6eddc9ab8866d05fa27ef3d2f171fcb2a240aa68f7242b3',
    name: 'David Diaz',
    studios: null,
    role: 'admin'
  },

  // ── Corporate (SWEAT440) — full access ────────────────────────────────────
  'matt@sweat440.com': {
    passwordHash: '92fc58cc5d4c65dc0e729ab464685f1e98ca864d367b7ead975316143eda4b98',
    name: 'Matt Miller',
    studios: null,
    role: 'admin'
  },
  'cody@sweat440.com': {
    passwordHash: '1795997e9ff59e77d14b1539574b29538f318fc4c3f40418498810a4610db22b',
    name: 'Cody Patrick',
    studios: null,
    role: 'corporate'
  },
  'liz@sweat440.com': {
    passwordHash: 'd7e6cbbfbf54323003090f4c38df5df9cf16068d9a69d4139adfb33c1261eebb',
    name: 'Liz Schmidt',
    studios: null,
    role: 'corporate'
  },
  'valeria@sweat440.com': {
    passwordHash: '0a608db52dbac47efd7d82bbb5ca50f43d3787303323c539238b6241bcb2548c',
    name: 'Valeria Vallejo',
    studios: null,
    role: 'corporate'
  },
  'ryan@sweat440.com': {
    passwordHash: 'a55c5acee96d109db1b9dc660e624dfb0e8b5b6647b5e27797f149019897cf8e',
    name: 'Ryan Hawell',
    studios: null,
    role: 'corporate'
  },
  'scott@sweat440.com': {
    passwordHash: 'b61a400650e82386dec094ae527ad930ecfaafd2cdc9fe1a102110c6f666c3a1',
    name: 'Scott Kinsworthy',
    studios: null,
    role: 'corporate'
  },
  'ricardo@sweat440.com': {
    passwordHash: '28eb3c0cd68bbf4d712e0223ae9fd10c62c33499efc6d6d3463656ac80b670a5',
    name: 'Ricardo Martinez',
    studios: null,
    role: 'corporate'
  },
    'jose.vera@sweat440.com': {
    passwordHash: '109622880505b23e67fd78f3525ba1ecadfa8733e550c06521b738b1f87d82a2',
    name: 'Jose Vera',
    studios: null,
    role: 'corporate'
  },
  'jose@sweat440.com': {
    passwordHash: '296a0b062b2c363b5dc4264c2cc465376db6b43836be03207891ec9c78dec70b',
    name: 'Jose Vera',
    studios: null,
    role: 'corporate'
  },
  'bradford@sweat440.com': {
    passwordHash: 'e5dcb9554ead1a04112aa0da7d1c4cb84ed2392dbf6336d83d71b40a64e32967',
    name: 'Bradford Rahmlow',
    studios: null,
    role: 'corporate'
  },
  'corporate@sweat440.com': {
    passwordHash: 'ddd5c9061082705dc210837bf37f3bce4a7da96bd49915edd7ad99651d65be3b',
    name: 'SWEAT440 Corporate',
    studios: ['Miami Beach', 'Miami - Brickell'],
    role: 'corporate'
  },

  // ── Franchisees ───────────────────────────────────────────────────────────

  'digitalmarketing@sweat440.com': {
    passwordHash: '88082350dadbc545b5677894efbd991700073bf79eb7d78c3c583692124a7e96',
    name: 'Marketing',
    studios: null,
    role: 'franchisee'
  },

  'alex@sweat440.com': {
    passwordHash: '6148189bed46eac08ab1de3198b5b8fe4bc106155e9cc48db03f7c065ccf9112',
    name: 'Alex Avila',
    studios: [
      'Aventura',
      'Boca Raton',
      'North Miami',
      'NYC - Chelsea',
      'NYC - FiDi',
      'NYC - Park Slope',
      'West Palm Beach',
    ],
    role: 'franchisee'
  },

  'amanda@redxfit.com': {
    passwordHash: '89ff3f7d55e57b9dceb056ac191d750cae7a07af6f40bed8ea9e8ece229681f6',
    name: 'Amanda Hays',
    studios: [
      'Madison',
    ],
    role: 'franchisee'
  },

  'cdevarona5@gmail.com': {
    passwordHash: '0f63f8aaa25f0b7abf5d5f4dfcdb9bf6cfaa993366803d70c243342f6f6a24c2',
    name: 'Carlos de Varona',
    studios: [
      'Miami Lakes',
      'Miramar',
      'Pembroke Pines',
    ],
    role: 'franchisee'
  },

  'yadishajorge@gmail.com': {
    passwordHash: '6b284133ae4c9a984ad216884cc78b27595cf9c5153d3015be92486b9dc4d077',
    name: 'Yadisha Jorge',
    studios: [
      'Miami Lakes',
      'Miramar',
      'Pembroke Pines',
    ],
    role: 'franchisee'
  },

  'chelsie@localfavorite.com': {
    passwordHash: 'f763b08be9b8e5a877afb329b53ca528133150f34b199be51b1cdd123473f148',
    name: 'Chelsie DiPaolo',
    studios: [
      'Dallas - Uptown',
    ],
    role: 'franchisee'
  },

  'vstones1430@gmail.com': {
    passwordHash: '72d08b301960aabbbe6d011907bf4a0ddf2dc8f967fc833b40ba92ed811d5cb6',
    name: 'Erika Sanchez',
    studios: [
      'Coral Gables',
      'Naples - Mercato',
    ],
    role: 'franchisee'
  },

  'gabrielzimerik@gmail.com': {
    passwordHash: 'b7fadee2ec84787e0934f14ff2f7fdb7ac3cef8d1b24ac310c316ffed7fc12e9',
    name: 'Gabriel Zimeri',
    studios: [
      'Dallas - Prestonwood',
    ],
    role: 'franchisee'
  },

  'christopherzimeri@gmail.com': {
    passwordHash: '6f1e512fa61c2645395bd1fc475ebcfc82f604f459449aa3d6596ce008e57310',
    name: 'Christopher Zimeri',
    studios: [
      'Dallas - Prestonwood',
    ],
    role: 'franchisee'
  },

  'jmarcoventures@gmail.com': {
    passwordHash: '48b90caf00185b81e5efe47eb32b640e859a141ecdef1a1c9fe5c395cfa29cd9',
    name: 'Jeff Marco',
    studios: [
      'Reston',
    ],
    role: 'franchisee'
  },

  'jimmy@purefitnessmiami.com': {
    passwordHash: '813e908ad6a0fe2ee6fdab5c6e39b81c13dfc44bc281594093b1223fc25129cc',
    name: 'Jimmy Kassis',
    studios: [
      'Pinecrest - Palmetto Bay',
    ],
    role: 'franchisee'
  },

  'sandra@purefitnessmiami.com': {
    passwordHash: '8bc26096bcd9a8c8bd1bdaa0ee412e7d2a6200e52477bca1ef4d666493da1b13',
    name: 'Sandra Kassis',
    studios: [
      'Pinecrest - Palmetto Bay',
    ],
    role: 'franchisee'
  },

  'sweat440drphillips@gmail.com': {
    passwordHash: 'cdfd2e3690f21aa05859a2ef75ae9a93a7ca747dc7b959bc2bcae4ee569f1727',
    name: 'Julian Leon',
    studios: [
      'Orlando - Dr Phillips',
    ],
    role: 'franchisee'
  },

  'julianviejillo@hotmail.com': {
    passwordHash: '2f111735ba6e3c59137e7c9042336024af9e9340cc11c80b10f06e59d4b6db7b',
    name: 'Julian Viejillo',
    studios: [
      'Orlando - Dr Phillips',
    ],
    role: 'franchisee'
  },

  'kenfrei212@gmail.com': {
    passwordHash: '03bdbfac6f27623041bf2613259bb4807c623abf0580d266721f74f6deaf8881',
    name: 'Ken Frei',
    studios: [
      'Herriman',
    ],
    role: 'franchisee'
  },

  'kristen@sweat440eastchester.com': {
    passwordHash: '77164796aba3b5172f5aaee9fa1ca7ac8320dea1dbf358598851446983b754b0',
    name: 'Kristen Albert',
    studios: [
      'Eastchester',
    ],
    role: 'franchisee'
  },

  'jamie@sweat440eastchester.com': {
    passwordHash: '10b3805986c4370fb866c98ed16dfded4c9e0442fd33c6fd235ad34013e9bb6d',
    name: 'Jamie Albert',
    studios: [
      'Eastchester',
    ],
    role: 'franchisee'
  },

  'marcg@ubfpt.com': {
    passwordHash: 'f20dc60cc48796cd97f932ec6f3eb2616704a397dc6675c1e7f35dcbf5b94816',
    name: 'Marc Gralnick',
    studios: [
      'Coral Springs',
      'Deerfield Beach',
      'Fort Lauderdale - Las Olas',
      'Miami - Coconut Grove',
      'Miami - Upper East Side',
      'NYC - Chelsea',
      'NYC - FiDi',
      'NYC - Park Slope',
      'Aventura',
      'North Miami',
      'Boca Raton',
      'West Palm Beach',
    ],
    role: 'franchisee'
  },

  'chris@ubfpt.com': {
    passwordHash: 'e89705b7dad672bdb55f41cf5f7b45f85652b7568277fe5a5698ccb228fb21f0',
    name: 'Chris Schuck',
    studios: [
      'Coral Springs',
      'Deerfield Beach',
      'Fort Lauderdale - Las Olas',
      'Miami - Coconut Grove',
      'Miami - Upper East Side',
      'NYC - Chelsea',
      'NYC - FiDi',
      'NYC - Park Slope',
      'Aventura',
      'North Miami',
      'Boca Raton',
      'West Palm Beach',
    ],
    role: 'franchisee'
  },

  'markcacciaguida@gmail.com': {
    passwordHash: '29d15c3b8a75b4d27a929231aff5c05a8b9d71dbe4ffe1fabafff4056f0f6af8',
    name: 'Mark Cacciaguida',
    studios: [
      'Doral',
      'Miami - Midtown',
    ],
    role: 'franchisee'
  },

  'matt.bvre@gmail.com': {
    passwordHash: 'cee141226eb3053c9fdec51c9b4e703e90db3d98e11efdbbc4383e543a442d38',
    name: "Matt O'Connor",
    studios: [
      'Charlotte - Noda',
    ],
    role: 'franchisee'
  },

  'mgelrud@hotmail.com': {
    passwordHash: '17b8f79636a93de2aeae965c445eb93af65c4009007f39b870c3324e7cd6b4d1',
    name: 'Max Gelrud',
    studios: [
      'South Miami',
    ],
    role: 'franchisee'
  },

  'nmarco@marcoregion.com': {
    passwordHash: 'fb67db4c70cbf40834a15fa59d2da7c0de995a140d7e434bda0bdd318a72e9be',
    name: 'Nick Marco',
    studios: [
      'Middletown',
      'Ocean Township',
      'Old Bridge',
      'Toms River',
      'Wall Township',
    ],
    role: 'franchisee'
  },

  'pmarcus@catalyst-hp.com': {
    passwordHash: 'ca7dfd189a4ff966d779092c8bcf3d8711fd45b426e3043ac82ea54b7c7bd84e',
    name: 'Paul Marcus',
    studios: [
      'Austin - Highland',
      'Austin - Zilker',
    ],
    role: 'franchisee'
  },

  'fraser@catalyst-hp.com': {
    passwordHash: 'b3c5b192f9ad4aa4af20d8d16d96d123deb1195615c211a08b5a70aa95fd21d2',
    name: 'Fraser Quarterman',
    studios: [
      'Austin - Highland',
      'Austin - Zilker',
    ],
    role: 'franchisee'
  },

  'mraygoza@catalyst-hp.com': {
    passwordHash: '7cfbbe1ee8d4dd865d4c7af147a4be6d0f8eacb8712c9d00f54e046129fbc906',
    name: 'Michael R',
    studios: [
      'Austin - Highland',
      'Austin - Zilker',
    ],
    role: 'franchisee'
  },

  // ── Additional franchisee partners ───────────────────────────────────────

  'christina@solarisft.com': {
    passwordHash: '77fcf61dda7d1c1b9520361c7a06e5acda4bd793cc97358badac00622f7f10c3',
    name: 'Christina',
    studios: ['Aventura', 'North Miami', 'NYC - Chelsea', 'NYC - Park Slope', 'Boca Raton', 'West Palm Beach', 'NYC - FiDi'],
    role: 'franchisee'
  },
  'mmasco@marcoregion.com': {
    passwordHash: '3a8b51f8f75d41ddbeea76cca9abebd6290fa3a4f2cfc820bc4defba360378a8',
    name: 'Mike Masco',
    studios: ['Reston', 'Old Bridge', 'Middletown', 'Wall Township', 'Ocean Township', 'Toms River'],
    role: 'franchisee'
  },
  'dbogota@marcoregion.com': {
    passwordHash: 'b3a6e169a58615d33a8cd5131534c5e71babb9c6372ab06f22bd4f611810d230',
    name: 'Danny Bogota',
    studios: ['Reston', 'Old Bridge', 'Middletown', 'Wall Township', 'Ocean Township', 'Toms River'],
    role: 'franchisee'
  },
  'jjankowski@marcoregion.com': {
    passwordHash: '6d40df3b8d087027aa4a13fb0b8fd59cda4197bb0fc46d8d797f9ad7c8e7595d',
    name: 'Joe Jankowski',
    studios: ['Reston', 'Old Bridge', 'Middletown', 'Wall Township', 'Ocean Township', 'Toms River'],
    role: 'franchisee'
  },
  'sunny@s440dfw.com': {
    passwordHash: 'f3113e7fc45b328d3ca26109767c0d719ae9fb8009b02c935bcbd9b8c9bd5302',
    name: 'Sunny',
    studios: ['Dallas - Uptown'],
    role: 'franchisee'
  },
  'ron@summitfitnessgroup.com': {
    passwordHash: '1f4e649c3fd63ef83c313a9bffce4f1b7a7858a80ec687c19e41885d13fe5cb9',
    name: 'Ron',
    studios: ['Dunwoody'],
    role: 'franchisee'
  },
  'gretchen@summitfitnessgroup.com': {
    passwordHash: 'ba6c0eb0f170a5f6141e14cc66305c247936a721d7afff1504516a09cd2df372',
    name: 'Gretchen',
    studios: ['Dunwoody'],
    role: 'franchisee'
  },
  'samfrei1@gmail.com': {
    passwordHash: '52f02277d6f6ca87c2261f8fcdc4a6fcf94fc974129f3e5cac45d8d69626bf43',
    name: 'Samantha Frei',
    studios: ['Herriman'],
    role: 'franchisee'
  },
  'jamieperucci@gmail.com': {
    passwordHash: '514a0581443ccd94d8e39882141e8ada4b15a82e7790778cc098805c67654dae',
    name: 'Jamie Perucci',
    studios: ['Herriman'],
    role: 'franchisee'
  },
  'darrenperucci@gmail.com': {
    passwordHash: 'eb1abf91bb480cdcd76f0e2f2d157f92e75a3bec673a17eec8776812e86081cf',
    name: 'Darren Perucci',
    studios: ['Herriman'],
    role: 'franchisee'
  },
  'wes@wnnen.com': {
    passwordHash: 'edcaf11a17220d1a410928c44c75154f4ab713bec65df7a15dd6fb28c43d73b5',
    name: 'Wesley',
    studios: ['Fort Myers'],
    role: 'franchisee'
  },
  'breyes887@gmail.com': {
    passwordHash: '135e8620ae97d25807b3b03b3433007b429fe0ab74bfffc52804cfed2b904390',
    name: 'Beatriz Reyes',
    studios: ['Fort Myers'],
    role: 'franchisee'
  },
  'aron@kairosok.com': {
    passwordHash: '31e040693ac34c1460bd7fdc3d02992316e4a1779d18107dd1163336e8c3a42b',
    name: 'Aaron Deen',
    studios: ['OKC - Rose Creek'],
    role: 'franchisee'
  },
  'jerry.ambroise@gmail.com': {
    passwordHash: 'cc34f77f0839fc5d24cc0abc74bd5d68ca0966a0541fe575b435e3331be339fb',
    name: 'Jerry Ambroise',
    studios: ['Long Beach'],
    role: 'franchisee'
  },
  'eileen.m.clarke@gmail.com': {
    passwordHash: '152e42b452665aefdf7c61e2fb31a3be13425bd2f7598dba10eada0b35e0234a',
    name: 'Eileen Clarke',
    studios: ['Long Beach'],
    role: 'franchisee'
  },
  'william@clholdingsinc.com': {
    passwordHash: 'f10e0a2b5cfd6a6d54144c7b61cdee5544e3dc9c8e539bca666c632555b61cc3',
    name: 'William',
    studios: ['Burlington'],
    role: 'franchisee'
  },
  'douglas@clholdingsinc.com': {
    passwordHash: 'e3e5730a440eb2514251db6906db3d444a4fb958fdeb267f0f648e6ccd1e64e1',
    name: 'Douglas',
    studios: ['Burlington'],
    role: 'franchisee'
  },

  // ── Studio Managers (single-studio access) ───────────────────────────────

  'manager.aventura@sweat440.studio': {
    passwordHash: '543a9974fd6baec86fe04c77e5e9684a935aeeebe252c92be1719c29a2919ae6',
    name: 'Manager Aventura',
    studios: ['Aventura'],
    role: 'manager'
  },
  'manager.boca@sweat440.studio': {
    passwordHash: '2fe64fe4dfe27a18458df4dcb387d9641bf4d330fb79f7b428b6a09ffa59180e',
    name: 'Manager Boca Raton',
    studios: ['Boca Raton'],
    role: 'manager'
  },
  'manager.brickell@sweat440.studio': {
    passwordHash: '0f039658bc4bb85dfeea292bf89b9edafc8360b742792c47700761d8c1df2f90',
    name: 'Manager Miami - Brickell',
    studios: ['Miami - Brickell'],
    role: 'manager'
  },
  'manager.burlington@sweat440.studio': {
    passwordHash: '4102053461ad96a0919a551b5c80647e78b89af941a57dfcd95d0f4a55929418',
    name: 'Manager Burlington',
    studios: ['Burlington'],
    role: 'manager'
  },
  'manager.chelsea@sweat440.studio': {
    passwordHash: '4aa74ce63b4e8aaa0ed6dec6f5709564e5d17f07a8e9201303799c2c7ad71457',
    name: 'Manager NYC - Chelsea',
    studios: ['NYC - Chelsea'],
    role: 'manager'
  },
  'manager.coralsprings@sweat440.studio': {
    passwordHash: '40c89e4ea51f85c2404b06cbf8461a309cff94c9c966cedb8fa87a1f0aff8d7f',
    name: 'Manager Coral Springs',
    studios: ['Coral Springs'],
    role: 'manager'
  },
  'manager.dallasuptown@sweat440.studio': {
    passwordHash: 'e530fabccadb9e93fe7fbefa2fd39ddac66dd50c438e059b64e80bb93a97be7c',
    name: 'Manager Dallas - Uptown',
    studios: ['Dallas - Uptown'],
    role: 'manager'
  },
  'manager.deerfield@sweat440.studio': {
    passwordHash: 'ef0122f7b9a185899de6d44050c96f19e1750dc1307a40f1338a3d1929beedf4',
    name: 'Manager Deerfield Beach',
    studios: ['Deerfield Beach'],
    role: 'manager'
  },
  'manager.doral@sweat440.studio': {
    passwordHash: '88e68ad16270a6dd697fdff5abde0e0f5f61a2141ad53b29ba6633fd630695e6',
    name: 'Manager Doral',
    studios: ['Doral'],
    role: 'manager'
  },
  'manager.drphillips@sweat440.studio': {
    passwordHash: 'db122b4ebc5cd5c1699443ce5065e378658bee66cde2b808ca8f7bb91843287e',
    name: 'Manager Orlando - Dr Phillips',
    studios: ['Orlando - Dr Phillips'],
    role: 'manager'
  },
  'manager.dunwoody@sweat440.studio': {
    passwordHash: '2638ec0260444dc24c7e10660d8e93fb9bfc37d97db97f986fab2d858df3efd6',
    name: 'Manager Dunwoody',
    studios: ['Dunwoody'],
    role: 'manager'
  },
  'manager.eastchester@sweat440.studio': {
    passwordHash: '4e04a959734564eae29a6352c763da037bda910bb4b3c426b9251db9a8ad683d',
    name: 'Manager Eastchester',
    studios: ['Eastchester'],
    role: 'manager'
  },
  'manager.fidi@sweat440.studio': {
    passwordHash: 'edd0da8d4554497becff62c2bd75fcfb80df198e95786ec5ef44ff1fdcab7796',
    name: 'Manager NYC - FiDi',
    studios: ['NYC - FiDi'],
    role: 'manager'
  },
  'manager.fortmyers@sweat440.studio': {
    passwordHash: 'f5adc6f78450e785ba628f6d9093fdd1d8e4119097449b810db1854e05841ca5',
    name: 'Manager Fort Myers',
    studios: ['Fort Myers'],
    role: 'manager'
  },
  'manager.gables@sweat440.studio': {
    passwordHash: '1879bfc6ce015229ae17e6296973e289708585b8155a753b2d19e2d57a57efb0',
    name: 'Manager Coral Gables',
    studios: ['Coral Gables'],
    role: 'manager'
  },
  'manager.grove@sweat440.studio': {
    passwordHash: '229153e54d8ae2f0af2bdd0702ac7117ffe14cccf6447e8497d49d2b1347bc79',
    name: 'Manager Miami - Coconut Grove',
    studios: ['Miami - Coconut Grove'],
    role: 'manager'
  },
  'manager.herriman@sweat440.studio': {
    passwordHash: 'f34b741d16e7935a92d7dadf6deed0934fd324b589f4422d80f5ec0b629f1d26',
    name: 'Manager Herriman',
    studios: ['Herriman'],
    role: 'manager'
  },
  'manager.highland@sweat440.studio': {
    passwordHash: 'f36d21bb7b8af2fe4d3473ab8d7d3953245c3e8cdcad7dd5c89ae4acc4b860c7',
    name: 'Manager Austin - Highland',
    studios: ['Austin - Highland'],
    role: 'manager'
  },
  'manager.lasolas@sweat440.studio': {
    passwordHash: '9314ed64bca372d94d28dae08385ae69dcc57b5c4a75c97646295827268edc4f',
    name: 'Manager Fort Lauderdale - Las Olas',
    studios: ['Fort Lauderdale - Las Olas'],
    role: 'manager'
  },
  'manager.longbeach@sweat440.studio': {
    passwordHash: 'f56c35806ef658008f9e2234ddab7573ad698550238a0de9d0052db9f71be404',
    name: 'Manager Long Beach',
    studios: ['Long Beach'],
    role: 'manager'
  },
  'manager.madison@sweat440.studio': {
    passwordHash: '8bd1506f99c5f8c8542b399fbfcce6db801cb59c256b60f51f6bcf6944f6afb6',
    name: 'Manager Madison',
    studios: ['Madison'],
    role: 'manager'
  },
  'manager.mercato@sweat440.studio': {
    passwordHash: '50b9dd7e84e8151633c9eac644543e163329e3efb80ee180fe0cac83dcecdf75',
    name: 'Manager Naples - Mercato',
    studios: ['Naples - Mercato'],
    role: 'manager'
  },
  'mercato@sweat440.studio': {
    passwordHash: '88082350dadbc545b5677894efbd991700073bf79eb7d78c3c583692124a7e96',
    name: 'Manager Naples - Mercato',
    studios: ['Naples - Mercato'],
    role: 'manager'
  },
  'manager.miamilakes@sweat440.studio': {
    passwordHash: '1fa833e0b34fbca590ca637859caf92a1c8e94e434a84660244824034b2f037c',
    name: 'Manager Miami Lakes',
    studios: ['Miami Lakes'],
    role: 'manager'
  },
  'manager.middletown@sweat440.studio': {
    passwordHash: '5af93fe67c9a2f33861e5fe13e63949f4ba0fb6df1fe0eddb8befc3db8b0ba76',
    name: 'Manager Middletown',
    studios: ['Middletown'],
    role: 'manager'
  },
  'manager.midtownmiami@sweat440.studio': {
    passwordHash: '6345912ce6b2040a1d27b44874bb9ae74a07d5b034785cd0b1186fe640731fdd',
    name: 'Manager Miami - Midtown',
    studios: ['Miami - Midtown'],
    role: 'manager'
  },
  'manager.miramar@sweat440.studio': {
    passwordHash: 'fc0123cc9ebddb8e7b97ddcc18c16a9a0722f3221478632f41e7b8e33f5867fd',
    name: 'Manager Miramar',
    studios: ['Miramar'],
    role: 'manager'
  },
  'manager.noda@sweat440.studio': {
    passwordHash: 'e3ea1ee16b2b769841c65602f3dbc8e31b3290c8f083bf1077d0078c23a212c0',
    name: 'Manager Charlotte - NoDa',
    studios: ['Charlotte - Noda'],
    role: 'manager'
  },
  'manager.northmiami@sweat440.studio': {
    passwordHash: 'b29788e1ac2571caba642823a57757be6af756a6ed3d2199b3c3228109e224f3',
    name: 'Manager North Miami',
    studios: ['North Miami'],
    role: 'manager'
  },
  'manager.ocean@sweat440.studio': {
    passwordHash: '17c6b1637aefde6031bad0d7fb41d6393c29362afe7a5d8e720470b91f064b3f',
    name: 'Manager Ocean Township',
    studios: ['Ocean Township'],
    role: 'manager'
  },
  'manager.oldbridge@sweat440.studio': {
    passwordHash: '0fe6fb234a3cb169b47746494131fd66b08a9dd2741c023bcb2f10bcab77e252',
    name: 'Manager Old Bridge',
    studios: ['Old Bridge'],
    role: 'manager'
  },
  'manager.parkslope@sweat440.studio': {
    passwordHash: 'cc88730ae87a9f7cf4f93aac115d712e88148e8711d15551bc994b7c404a0439',
    name: 'Manager NYC - Park Slope',
    studios: ['NYC - Park Slope'],
    role: 'manager'
  },
  'manager.pinecrest@sweat440.studio': {
    passwordHash: '692bf7f3b845c8f8415d3a7d428e8a27313b546cde76834b82ff2b0819563eda',
    name: 'Manager Pinecrest',
    studios: ['Pinecrest - Palmetto Bay'],
    role: 'manager'
  },
  'manager.pines@sweat440.studio': {
    passwordHash: '42ce9e61f4c60a1a6130f5ae0505547a4a19bcd49f90355522c6c1d55d0106b5',
    name: 'Manager Pembroke Pines',
    studios: ['Pembroke Pines'],
    role: 'manager'
  },
  'manager.prestonwood@sweat440.studio': {
    passwordHash: 'f77661053e345d984291fd6307cf8313605f0b5f90f417e2dbf8782f6abc808b',
    name: 'Manager Dallas - Prestonwood',
    studios: ['Dallas - Prestonwood'],
    role: 'manager'
  },
  'manager.reston@sweat440.studio': {
    passwordHash: '71535f36c17de6f0bf197540a62bfd0294517f565055704ee32595e1e3d2279b',
    name: 'Manager Reston',
    studios: ['Reston'],
    role: 'manager'
  },
  'manager.rosecreek@sweat440.studio': {
    passwordHash: '695dcd64384ad9eb113c2eab595c8087ab419acc8465463392df8cd33d402840',
    name: 'Manager OKC - Rose Creek',
    studios: ['OKC - Rose Creek'],
    role: 'manager'
  },
  'manager.sobe@sweat440.studio': {
    passwordHash: 'ef8efc22c1c0f3bbbd4f17141583dcf3549e0b94de40ecd25711e5da9c0ee524',
    name: 'Manager Miami Beach',
    studios: ['Miami Beach'],
    role: 'manager'
  },
  'manager.southmiami@sweat440.studio': {
    passwordHash: 'a5b9fc8a9af01ce8bd23d3c2a543f24d7d301a834bf1547fda4e1c11222b3aa5',
    name: 'Manager South Miami',
    studios: ['South Miami'],
    role: 'manager'
  },
  'manager.tomsriver@sweat440.studio': {
    passwordHash: '539ecbe402ddc78c57bc4f863c760630568d2f1d054ffcf2dbb6796791cb8fd0',
    name: 'Manager Toms River',
    studios: ['Toms River'],
    role: 'manager'
  },
  'manager.uppereastside@sweat440.studio': {
    passwordHash: '23a4e130fb7c496b5901a5ce4967584021cb154dc756c53dff123114dd567f57',
    name: 'Manager Miami - Upper East Side',
    studios: ['Miami - Upper East Side'],
    role: 'manager'
  },
  'manager.wall@sweat440.studio': {
    passwordHash: 'da0e4f715bf551adc65f17da115e2e29838bd66d702de74626fdb31f158dacdd',
    name: 'Manager Wall Township',
    studios: ['Wall Township'],
    role: 'manager'
  },
  'manager.wpb@sweat440.studio': {
    passwordHash: '6fd83d84a184433a721dd85e8084ca56879b9a9715d73c8c3ee8ed157c5760b9',
    name: 'Manager West Palm Beach',
    studios: ['West Palm Beach'],
    role: 'manager'
  },
  'manager.zilker@sweat440.studio': {
    passwordHash: 'd9372b8e75d41734c1157ead7ff6c9766499cd39035c817a0fa6c0bd6478d38a',
    name: 'Manager Austin - Zilker',
    studios: ['Austin - Zilker'],
    role: 'manager'
  },

};
