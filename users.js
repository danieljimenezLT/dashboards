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
  'jamie.westall@leadteam.com': {
    passwordHash: 'd51efb736aca536431204927a0dc30e4909d49b809733b7ac6b1227610b6e679',
    name: 'Jamie Westall',
    studios: null,
    role: 'admin'
  },
  'sabastian.arango@leadteam.com': {
    passwordHash: '82251cee9a5262e36d13362d1b86e38fb0fb3314570ace33171d7aa3f08c33de',
    name: 'Sabastian Arango',
    studios: null,
    role: 'admin'
  },
  'daniel.jimenez@leadteam.com': {
    passwordHash: '24851852c95b5d223f697bb0a9796f93ffb7be334b3951cef647dadd61b33729',
    name: 'Daniel Jimenez',
    studios: null,
    role: 'admin'
  },

  // ── Corporate (SWEAT440) — full access ────────────────────────────────────
  'matt@sweat440.com': {
    passwordHash: '92fc58cc5d4c65dc0e729ab464685f1e98ca864d367b7ead975316143eda4b98',
    name: 'Matt Miller',
    studios: null,
    role: 'corporate'
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
  'jose@sweat440.com': {
    passwordHash: '296a0b062b2c363b5dc4264c2cc465376db6b43836be03207891ec9c78dec70b',
    name: 'Jose Vera',
    studios: null,
    role: 'corporate'
  },

  // ── Franchisees ───────────────────────────────────────────────────────────

  'alex@sweat440.com': {
    passwordHash: '32620a96dfe2cf41f26c5751dd0197ff0235421dec2bd8b3f503f187a2665c76',
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
    passwordHash: 'd51efb736aca536431204927a0dc30e4909d49b809733b7ac6b1227610b6e679',
    name: 'Amanda Hays',
    studios: [
      'Madison',
    ],
    role: 'franchisee'
  },

  'cdevarona5@gmail.com': {
    passwordHash: '82251cee9a5262e36d13362d1b86e38fb0fb3314570ace33171d7aa3f08c33de',
    name: 'Carlos de Varona',
    studios: [
      'Miami Lakes',
      'Miramar',
      'Pembroke Pines',
    ],
    role: 'franchisee'
  },

  'chelsie@localfavorite.com': {
    passwordHash: '24851852c95b5d223f697bb0a9796f93ffb7be334b3951cef647dadd61b33729',
    name: 'Chelsie DiPaolo',
    studios: [
      'Dallas - Uptown',
    ],
    role: 'franchisee'
  },

  'vstones1430@gmail.com': {
    passwordHash: '1795997e9ff59e77d14b1539574b29538f318fc4c3f40418498810a4610db22b',
    name: 'Erika Sanchez',
    studios: [
      'Coral Gables',
      'Naples - Mercato',
    ],
    role: 'franchisee'
  },

  'gabrielzimerik@gmail.com': {
    passwordHash: 'd7e6cbbfbf54323003090f4c38df5df9cf16068d9a69d4139adfb33c1261eebb',
    name: 'Gabriel Zimeri',
    studios: [
      'Dallas - Prestonwood',
    ],
    role: 'franchisee'
  },

  'jmarcoventures@gmail.com': {
    passwordHash: '0a608db52dbac47efd7d82bbb5ca50f43d3787303323c539238b6241bcb2548c',
    name: 'Jeff Marco',
    studios: [
      'Reston',
    ],
    role: 'franchisee'
  },

  'jimmy@purefitnessmiami.com': {
    passwordHash: 'a55c5acee96d109db1b9dc660e624dfb0e8b5b6647b5e27797f149019897cf8e',
    name: 'Jimmy Kassis',
    studios: [
      'Pinecrest - Palmetto Bay',
    ],
    role: 'franchisee'
  },

  'sweat440drphillips@gmail.com': {
    passwordHash: 'b61a400650e82386dec094ae527ad930ecfaafd2cdc9fe1a102110c6f666c3a1',
    name: 'Julian Leon',
    studios: [
      'Orlando - Dr Phillips',
    ],
    role: 'franchisee'
  },

  'kenfrei212@gmail.com': {
    passwordHash: '28eb3c0cd68bbf4d712e0223ae9fd10c62c33499efc6d6d3463656ac80b670a5',
    name: 'Ken Frei',
    studios: [
      'Herriman',
    ],
    role: 'franchisee'
  },

  'kristen@sweat440eastchester.com': {
    passwordHash: '296a0b062b2c363b5dc4264c2cc465376db6b43836be03207891ec9c78dec70b',
    name: 'Kristen Albert',
    studios: [
      'Eastchester',
    ],
    role: 'franchisee'
  },

  'marcg@ubfpt.com': {
    passwordHash: 'ddd5c9061082705dc210837bf37f3bce4a7da96bd49915edd7ad99651d65be3b',
    name: 'Marc Gralnick',
    studios: [
      'Coral Springs',
      'Deerfield Beach',
      'Fort Lauderdale - Las Olas',
      'Miami - Coconut Grove',
      'Miami - Upper East Side',
    ],
    role: 'franchisee'
  },

  'markcacciaguida@gmail.com': {
    passwordHash: '15743531a3962a0aff9eb020754e49cb58ef8ab6ed81ddfcb674572e60c62f8a',
    name: 'Mark Cacciaguida',
    studios: [
      'Doral',
      'Miami - Midtown',
    ],
    role: 'franchisee'
  },

  'matt.bvre@gmail.com': {
    passwordHash: 'd88941531023bac8a31eb314ef70f3f68384cb4d6eac6e11bcd72bfadc94857d',
    name: "Matt O'Connor",
    studios: [
      'Charlotte - Noda',
    ],
    role: 'franchisee'
  },

  'mgelrud@hotmail.com': {
    passwordHash: 'd335bf42e39a6a32a89f0af712959f8b3b99c33b7fd5d89c2eba678e53eece73',
    name: 'Max Gelrud',
    studios: [
      'South Miami',
    ],
    role: 'franchisee'
  },

  'nmarco@marcoregion.com': {
    passwordHash: 'bc791077b452b49030913ecff3162274e589a6db036dc2af28aff2c8dad78fb1',
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
    passwordHash: '852789ef58b107d14ac27bd472aa381d80d198b4ac55132799387b110bd93b76',
    name: 'Paul Marcus',
    studios: [
      'Austin - Highland',
      'Austin - Zilker',
    ],
    role: 'franchisee'
  },

};
