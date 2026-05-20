package channel

// Curated word lists for friendly peer-name generation. The corpus is
// deliberately gentle, work-safe, and visually distinct: ~200 adjectives and
// ~200 nouns, all lowercase, no hyphens, no spaces. Combined with the 3-char
// base36 suffix (UniqueName) the keyspace is ~200 * 200 * 36^3 ≈ 1.86 * 10^9
// — same-token collisions are essentially impossible in practice, so the
// adapter's collision-retry loop is a defence-in-depth backstop, not the
// primary safety mechanism.
//
// Add words by appending — order is irrelevant; nothing keys off index.

// nameAdjectives is the adjective half of the friendly-name dictionary.
var nameAdjectives = []string{
	"agile", "amber", "ancient", "azure", "balmy", "bashful", "blissful",
	"bold", "bouncy", "brave", "breezy", "bright", "brisk", "bronze",
	"bubbly", "calm", "candid", "careful", "cheerful", "chipper", "clever",
	"cosy", "cool", "copper", "cosmic", "crimson", "crisp", "curious",
	"daring", "dapper", "dawn", "dazzling", "deft", "dewy", "diligent",
	"dreamy", "dusky", "eager", "earnest", "easy", "elated", "electric",
	"elegant", "emerald", "epic", "exotic", "fair", "fancy", "feisty",
	"fearless", "festive", "fiery", "flaxen", "fleecy", "fleet", "fluffy",
	"flying", "forest", "fresh", "friendly", "frosty", "funny", "gallant",
	"gentle", "giddy", "gilded", "glassy", "gleaming", "glowing", "golden",
	"graceful", "grand", "grateful", "grassy", "happy", "hardy", "harmonic",
	"hazel", "hearty", "heroic", "honest", "humble", "icy", "indigo",
	"jade", "jaunty", "jolly", "joyful", "jovial", "keen", "kind",
	"lavender", "lean", "light", "lilac", "limber", "limpid", "lively",
	"loyal", "lucky", "lumen", "lunar", "marble", "mellow", "merry",
	"midnight", "mighty", "mild", "mindful", "minty", "misty", "modest",
	"mossy", "mythic", "nautical", "neat", "nimble", "noble", "north",
	"olive", "opal", "orange", "outer", "pacific", "patient", "peachy",
	"pearl", "perky", "placid", "plucky", "plum", "polar", "polished",
	"prairie", "prancing", "primal", "proper", "proud", "prudent", "quaint",
	"quick", "quiet", "quirky", "radiant", "rapid", "raven", "ready",
	"regal", "ringed", "river", "robust", "rosy", "ruby", "rugged",
	"rustic", "sable", "sacred", "salty", "sandy", "sapphire", "savvy",
	"scarlet", "scenic", "secret", "serene", "shady", "sharp", "shimmer",
	"shiny", "silent", "silken", "silver", "skyward", "sleek", "smart",
	"smiling", "smoky", "smooth", "snowy", "soaring", "solar", "solid",
	"sound", "south", "sparkly", "spirited", "spry", "stalwart", "starry",
	"steady", "stellar", "stoic", "stout", "summery", "sunlit", "sunny",
	"super", "swift", "tangy", "teal", "tender", "thrifty", "tidy", "tidal",
	"timely", "tranquil", "trusty", "twilight", "vast", "velvet", "verdant",
	"vibrant", "vigil", "vintage", "violet", "vivid", "warm", "wandering",
	"wavy", "whimsical", "wild", "willing", "windswept", "winsome", "wise",
	"witty", "woolen", "yonder", "youthful", "zealous", "zen", "zesty",
}

// nameNouns is the noun half of the friendly-name dictionary.
var nameNouns = []string{
	"acorn", "albatross", "alder", "alpaca", "amber", "anchor", "antler",
	"apricot", "arrow", "ash", "aspen", "auk", "aurora", "axis", "azalea",
	"badger", "bamboo", "banner", "barley", "basil", "beacon", "beaver",
	"beetle", "bell", "birch", "bison", "blossom", "bluebell", "bobcat",
	"boulder", "brambler", "branch", "brook", "buffalo", "bumble", "burrow",
	"cabin", "cactus", "calla", "camel", "candle", "canyon", "cardinal",
	"cedar", "chamois", "cheetah", "cherry", "chestnut", "cinder", "clover",
	"coast", "cobalt", "comet", "compass", "condor", "coral", "cottage",
	"cougar", "coyote", "cranberry", "crocus", "cypress", "daisy", "dandelion",
	"deer", "delta", "dingo", "dolphin", "dragonfly", "drift", "dune",
	"eagle", "ember", "emu", "estuary", "falcon", "fawn", "fennec", "fern",
	"ferret", "field", "finch", "firefly", "flamingo", "flax", "fjord",
	"foxglove", "frost", "galaxy", "garnet", "gazelle", "gecko", "gem",
	"geyser", "ginger", "glacier", "glade", "gleam", "globe", "gopher",
	"granite", "grebe", "grove", "gull", "harbor", "harmony", "harvest",
	"hawk", "haven", "hazel", "heath", "heron", "hill", "hollow", "honey",
	"hornbill", "horizon", "iris", "ivory", "ivy", "jackal", "jasper",
	"jay", "junco", "juniper", "kelp", "kestrel", "kingfisher", "kit",
	"koala", "lagoon", "lantern", "lark", "lavender", "leaf", "lemur",
	"lichen", "lighthouse", "lily", "lime", "linden", "lion", "lupine",
	"lynx", "magnolia", "mallow", "maple", "marigold", "marmot", "marsh",
	"meadow", "mesa", "midge", "mink", "minnow", "mirage", "moose", "morel",
	"moth", "mountain", "muffin", "narwhal", "nebula", "newt", "nimbus",
	"nook", "nova", "oak", "oasis", "ocelot", "ocean", "olive", "onyx",
	"opal", "orca", "orchard", "orchid", "osprey", "otter", "owl", "panda",
	"pansy", "panther", "parrot", "partridge", "pebble", "pelican", "petal",
	"phoenix", "pika", "pine", "plover", "plum", "pollen", "pond", "poppy",
	"prairie", "puffin", "quail", "quartz", "quokka", "rabbit", "raccoon",
	"raven", "reef", "reindeer", "ridge", "river", "robin", "rookery", "rose",
	"sage", "salmon", "satellite", "savanna", "seal", "sequoia", "shore",
	"shrike", "skunk", "skyline", "sparrow", "spire", "starling", "stork",
	"stream", "summit", "swan", "tamarack", "teak", "thicket", "thistle",
	"tortoise", "tulip", "tundra", "valley", "vega", "violet", "vista",
	"vixen", "wallaby", "warbler", "waterfall", "willow", "wolf", "wombat",
	"woodland", "wren", "yarrow", "yew", "zebra", "zephyr", "zinnia",
}
