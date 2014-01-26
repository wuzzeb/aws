{-# LANGUAGE GeneralizedNewtypeDeriving, DeriveGeneric, UndecidableInstances #-}
module Aws.DynamoDb.Commands.Items where

import Aws.Core
import Aws.DynamoDb.Core
import Control.Applicative
import Data.Aeson
import Data.Aeson.Types
import Data.Conduit (monadThrow)
import GHC.Generics (Generic)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Base64.Lazy as Base64
import qualified Data.Text as T
import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.HashMap.Lazy as M
import qualified Network.HTTP.Conduit as HTTP

-- | An item attribute value.
data AttrValue = ValBlob BL.ByteString
               | ValBlobSet [BL.ByteString]
               | ValNumber Int
               | ValNumberSet [Int]
               | ValString T.Text
               | ValStringSet [T.Text]
    deriving Show
instance ToJSON AttrValue where
    toJSON (ValBlob x) = object [ "B" .= (TL.decodeUtf8 $ Base64.encode x) ]
    toJSON (ValBlobSet x) = object [ "BS" .= map (TL.decodeUtf8 . Base64.encode) x ]
    toJSON (ValNumber x) = object [ "N" .= x ]
    toJSON (ValNumberSet x) = object [ "NS" .= x ]
    toJSON (ValString x) = object [ "S" .= x ]
    toJSON (ValStringSet x) = object [ "SS" .= x ]
instance FromJSON AttrValue where
    parseJSON = withObject "Attribute value must be an object" $ \o ->
          ValBlob . Base64.decodeLenient . TL.encodeUtf8 <$> o .: "B"
      <|> ValBlobSet . map (Base64.decodeLenient . TL.encodeUtf8) <$> o .: "BS"
      <|> ValNumber . read <$> o .: "N" -- AWS returns numbers as strings
      <|> ValNumberSet . map read <$> o .: "NS"
      <|> ValString <$> o .: "S"
      <|> ValStringSet <$> o .: "SS"
      <|> fail ("Attribute must have B, BS, N, NS, S, SS: " ++ show o)

-- | A class of things convertable to an attribute value.  Primarily intended to be used with
-- '(~=)'.
class ToAttrValue v where
    itemValue :: v -> AttrValue
instance ToAttrValue B.ByteString where
    itemValue b = ValBlob $ BL.fromChunks [b]
instance ToAttrValue [B.ByteString] where
    itemValue bs = ValBlobSet $ map (\b -> BL.fromChunks [b]) bs
instance ToAttrValue BL.ByteString where
    itemValue = ValBlob
instance ToAttrValue [BL.ByteString] where
    itemValue = ValBlobSet
instance ToAttrValue Int where
    itemValue = ValNumber
instance ToAttrValue [Int] where
    itemValue = ValNumberSet
instance ToAttrValue T.Text where
    itemValue = ValString
instance ToAttrValue [T.Text] where
    itemValue = ValStringSet

-- | A class of things parsable from an attribute value.  Primarily intended to be used with '(~:)'.
class FromAttrValue v where
    parseAttrValue :: AttrValue -> Parser v
instance FromAttrValue B.ByteString where
    parseAttrValue (ValBlob b) = return $ BL.toStrict b
    parseAttrValue _ = fail "Expecting blob"
instance FromAttrValue [B.ByteString] where
    parseAttrValue (ValBlobSet b) = return $ map BL.toStrict b
    parseAttrValue _ = fail "Expecting blob set"
instance FromAttrValue BL.ByteString where
    parseAttrValue (ValBlob b) = return b
    parseAttrValue _ = fail "Expecting blob"
instance FromAttrValue [BL.ByteString] where
    parseAttrValue (ValBlobSet b) = return b
    parseAttrValue _ = fail "Expecting blob set"
instance FromAttrValue Int where
    parseAttrValue (ValNumber i) = return i
    parseAttrValue _ = fail "Expecting number"
instance FromAttrValue [Int] where
    parseAttrValue (ValNumberSet i) = return i
    parseAttrValue _ = fail "Expecting number set"
instance FromAttrValue T.Text where
    parseAttrValue (ValString t) = return t
    parseAttrValue _ = fail "Expecting string"
instance FromAttrValue [T.Text] where
    parseAttrValue (ValStringSet t) = return t
    parseAttrValue _ = fail "Expecting string set"

newtype Item = Item { itemMap :: M.HashMap T.Text AttrValue }
    deriving (Show)
instance ToJSON Item where
    toJSON (Item items) = toJSON items
instance FromJSON Item where
    parseJSON o = Item <$> parseJSON o

type Attr = (T.Text, AttrValue)

-- | Construct an item from a sequence of attributes.  Attribute names later in the list override
-- earlier ones.
item :: [(T.Text,AttrValue)] -> Item
item = Item . M.fromList

-- | The primary means of constructing an attribute for use with 'item'.
(~=) :: ToAttrValue v => T.Text -> v -> Attr
name ~= v = (name, itemValue v)

-- | You should make your data structures instances of 'ToItem' and 'FromItem'.  For example,
--
-- >data Spaceship = Spaceship {
-- >    spaceshipName :: T.Text
-- >  , spaceshipPilot :: T.Text
-- >  , spaceshipFuel :: Int
-- >} deriving (Show)
-- >
-- >instance ToItem Spaceship where
-- >    toItem s = item [ "name" ~= spaceshipName s
-- >                    , "pilot" ~= spaceshipPilot s
-- >                    , "fuel" ~= spaceshipFuel s
-- >                    ]
class ToItem item where
    toItem :: item -> Item
instance ToItem Item where
    toItem = id
instance ToItem (M.HashMap T.Text AttrValue) where
    toItem = Item

-- | Lookup an attribute by name.
(~:) :: FromAttrValue v => Item -> T.Text -> Parser v
(Item i) ~: name = case M.lookup name i of
                    Just v -> parseAttrValue v
                    Nothing -> fail $ "Unable to find attribute " ++ T.unpack name

-- | Parse an item into your data structure.  For example,
--
-- >instance FromItem Spaceship where
-- >    parseItem i = Spaceship <$> i ~: "name"
-- >                            <*> i ~: "pilot"
-- >                            <*> i ~: "fuel"
class FromItem v where
    parseItem :: Item -> Parser v
instance FromItem Item where
    parseItem = return
instance FromItem (M.HashMap T.Text AttrValue) where
    parseItem (Item i) = return i


data ReturnConsumedCapacity = ReturnIndexesCapacity
                            | ReturnTotalCapacity
                            | ReturnNoCapacity
    deriving Show
instance ToJSON ReturnConsumedCapacity where
    toJSON ReturnIndexesCapacity = String "INDEXES"
    toJSON ReturnTotalCapacity = String "TOTAL"
    toJSON ReturnNoCapacity = String "NONE"

data GetKey = GetHash Attr
            | GetHashAndRange Attr Attr
    deriving Show
instance ToJSON GetKey where
    toJSON (GetHash (name, val)) = object [name .= val]
    toJSON (GetHashAndRange (n1,v1) (n2,v2)) = object [n1 .= v1, n2 .= v2]

-------------------------------------------------------------------------------
--- Commands
-------------------------------------------------------------------------------

data GetItem
    = GetItem {
        getTableName :: T.Text
      , getKey :: GetKey
      , getAttributesToGet :: [T.Text]
      , getConsistentRead :: Bool
      --, getReturnConsumedCapacity :: ReturnConsumedCapacity
      }
    deriving (Show, Generic)
instance ToJSON GetItem where
    toJSON gi
        = object $ [ "TableName" .= getTableName gi
                   , "Key" .= getKey gi
                   , "ConsistentRead" .= getConsistentRead gi
                   --, "ReturnedConsumedCapacity" .= getReturnConsumedCapacity gi
                   ]
                   ++
                   if null (getAttributesToGet gi)
                    then []
                    else [ "AttributesToGet" .= getAttributesToGet gi ]
    
-- | Get from a table that uses only a hash key and not a range key.  This returns all attributes
-- using a non-consistent read.  When used with 'Aws.Aws.simpleAws', the return value can be
-- anything that is an instance of 'FromItem'.  For example,
--
-- >ship <- simpleAws cfg dcfg $ get "spaceships" ("name" ~= "Planet Express Ship")
-- >putStrLn $ "Pilot is " ++ T.unpack (spaceshipPilot ship)
get :: T.Text -> Attr -> GetItem
get table key = GetItem table (GetHash key) [] False

-- | Similar to 'get' but for a table that has both a hash and a range key.
get' :: T.Text -> Attr -> Attr -> GetItem
get' table hkey rkey = GetItem table (GetHashAndRange hkey rkey) [] False

-- | ServiceConfiguration: 'DyConfiguration'
instance SignQuery GetItem where
    type ServiceConfiguration GetItem = DyConfiguration
    signQuery = dySignQuery "GetItem"

data GetReturn = GetReturn { getRetItem :: Item }
    deriving (Show)
instance FromJSON GetReturn where
    parseJSON = withObject "Return must be an object" $ \g -> GetReturn <$> g .: "Item"

newtype GetItemResult item = GetItemResult { returnedItem :: item }
    deriving Show
instance FromItem item => ResponseConsumer r (GetItemResult item) where
    type ResponseMetadata (GetItemResult item) = DyMetadata
    responseConsumer _ _ resp = do
        (GetReturn i) <- dyResponseConsumer resp
        case parse parseItem i of
            Success i' -> return $ GetItemResult i'
            Error err -> monadThrow $ DyError (HTTP.responseStatus resp) "" err

instance FromItem item => AsMemoryResponse (GetItemResult item) where
    type MemoryResponse (GetItemResult item) = item
    loadToMemory = return . returnedItem

instance FromItem item => Transaction GetItem (GetItemResult item)

data PutItem
    = PutItem {
        putTableName :: T.Text
      , putItem :: Item
      , putExpected :: Maybe Item
      , putExpectedToNotExist :: [T.Text]
      --, putReturnConsumedCapacity :: ReturnConsumedCapacity
      }
    deriving (Show)
instance ToJSON PutItem where
    toJSON p = object $ [ "TableName" .= putTableName p
                        , "Item" .= putItem p
                        , "ReturnValues" .= ("NONE" :: T.Text)
                        --, "ReturnConsumedCapacity" .= putReturnConsumedCapacity p
                        ]
                        ++
                        expected
        where
            notexist = M.fromList [ (n, object [ "Exists" .= False]) | n <- putExpectedToNotExist p]
            vals = maybe M.empty (M.map (\v -> object [ "Value" .= v ]) . itemMap) $ putExpected p
            expM = notexist `M.union` vals
            expected = if M.null expM
                            then []
                            else [ "Expected" .= expM ]

-- | ServiceConfiguration: 'DyConfiguration'
instance SignQuery PutItem where
    type ServiceConfiguration PutItem = DyConfiguration
    signQuery = dySignQuery "PutItem"

-- | Store an item, with no expected attributes and not returning the old item.
put :: T.Text -> Item -> PutItem
put table i = PutItem table i Nothing []

data PutItemResult = PutItemResult
    deriving (Show)
instance FromJSON PutItemResult where
    parseJSON _ = return PutItemResult

instance ResponseConsumer r PutItemResult where
    type ResponseMetadata PutItemResult = DyMetadata
    responseConsumer _ _ = dyResponseConsumer
instance AsMemoryResponse PutItemResult where
    type MemoryResponse PutItemResult = ()
    loadToMemory _ = return ()

instance Transaction PutItem PutItemResult
