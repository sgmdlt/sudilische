
import cv2
import numpy as np
import tensorflow as tf
import tf_keras.backend as K
from tf_keras import layers, models
from tf_keras.saving import load_model


class CaptchaSolver:
    def __init__(self):
        # Загружаем модель
        self.loaded_model = load_model("sud_rnn_0.6.1")
        self.prediction_model = models.Model(
            self.loaded_model.get_layer(name="image").input,
            self.loaded_model.get_layer(name="dense2").output)
        self.characters = "0123456789"
        self.char_to_num = layers.StringLookup(vocabulary=list(self.characters), mask_token=None)
        self.num_to_char = layers.StringLookup(vocabulary=self.char_to_num.get_vocabulary(),
                                  mask_token=None,
                                  invert=True)

    def _prepare_img(self, img):
        exclude_colors = np.array([[153, 102, 0]])
        c = np.unique(np.unique(img[:, 0:5], axis=0).reshape(-1, 3), axis=0)
        c2 = np.unique(np.unique(img[:, 94:99], axis=0).reshape(-1, 3), axis=0)
        c3 = np.unique(np.unique(img[0 - 2], axis=0).reshape(-1, 3), axis=0)
        c4 = np.unique(np.unique(img[28 - 29], axis=0).reshape(-1, 3), axis=0)
        colors = np.concatenate((c, c2, c3, c4), axis=0)
        res_img = np.copy(img)
        for c in colors:
            if (exclude_colors == c).all(1).any():
                continue
            res_img[res_img == c] = 255
        return res_img

    def _encode_single_sample(self, img, label):
        img_width = 200
        img_height = 60

        img = tf.convert_to_tensor(img, dtype=tf.uint8)
        img = tf.image.rgb_to_grayscale(img)
        img = tf.image.convert_image_dtype(img, tf.float32)
        img = tf.image.resize(img, [img_height, img_width])
        img = tf.transpose(img, perm=[1, 0, 2])
        label = self.char_to_num(tf.strings.unicode_split(label, input_encoding="UTF-8"))
        return {"image": img, "label": label}

    def _decode_batch_predictions(self, pred):   
        input_len = np.ones(pred.shape[0]) * pred.shape[1]
        results = K.ctc_decode(pred, input_length=input_len,greedy=True)[0][0][:, :5]
        output_text = []
        for res in results:
            res = tf.strings.reduce_join(self.num_to_char(res)).numpy().decode("utf-8")
            output_text.append(res)
        return output_text

    async def solve_captcha(self, captcha_image_bytes: bytes) -> str:
        """
        Решает капчу, распознавая текст на изображении.

        :param captcha_image_bytes: Байты изображения капчи.
        :return: Распознанный текст капчи.
        """
        # Декодируем изображение
        image = cv2.imdecode(np.frombuffer(captcha_image_bytes, np.uint8), cv2.IMREAD_COLOR)
        # Подготовка изображения
        img = self._prepare_img(image)
        # Распознавание капчи
        ds = tf.data.Dataset.from_tensor_slices(([img], [""]))
        ds = (ds.map(self._encode_single_sample,
                     num_parallel_calls=tf.data.AUTOTUNE).batch(1).prefetch(
                         buffer_size=tf.data.AUTOTUNE))
        pred = self.prediction_model.predict(ds, verbose=0)
        pred_texts = self._decode_batch_predictions(pred)
        captcha_result = pred_texts[0]
        return captcha_result
