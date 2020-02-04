# shameless copy from https://github.com/YontiLevin/Embeddings2Image/blob/master/e2i/Maatens_tsne.py

import numpy as np
import sklearn
from tqdm import tqdm
from scipy.spatial.distance import cdist
from lapjv import lapjv
import umap as UMAP

MAX = 10000.*10000.*100.
def umap(distance_matrix, **parameters):
    v = np.minimum(MAX, distance_matrix)
    #n = np.square(int(np.ceil(np.sqrt(v.shape[0]))))
    #v = np.pad(v, ((0,n-v.shape[0]), (0, n-v.shape[0])), mode='constant', constant_values=MAX)
    v = UMAP.UMAP(metric="precomputed", **parameters).fit_transform(v)
    return v
    #out = np.ones((out_dim*out_res, out_dim*out_res, 3))
def tsne(distance_matrix):
    #v = 1/distance_matrix
    v = np.minimum(MAX, distance_matrix)
    #n = np.square(int(np.ceil(np.sqrt(v.shape[0]))))
    #v = np.pad(v, ((0,n-v.shape[0]), (0, n-v.shape[0])), mode='constant', constant_values=0)
    v = sklearn.manifold.TSNE(metric="precomputed").fit_transform(v)
    #v = TSNE(v, no_dims=2, max_iter=max_iter)
    return v
def grid(poses, padding_ratio=4/3):
    v = poses
    v -= v.min(axis=0)
    v /= v.max(axis=0)
    #out_dim = int(np.sqrt(len(v)))
    out_dim = int(np.ceil(np.sqrt(len(poses)*padding_ratio)))

    grid = np.dstack(np.meshgrid(np.linspace(0, 1, out_dim), np.linspace(0, 1, out_dim))).reshape(-1, 2)
    #grid = np.dstack(np.meshgrid(np.linspace(0, out_dim-1, out_dim), np.linspace(0, out_dim-1, out_dim))).reshape(-1, 2)
    cost_matrix = cdist(v, grid, "sqeuclidean")
    cost_matrix = cost_matrix * (100000 / cost_matrix.max())
    cost_matrix = np.pad(cost_matrix, ((0, out_dim*out_dim - len(poses)), (0, 0)), mode='constant', constant_values=0).astype(np.float32)
    row_asses, col_asses, _ = lapjv(cost_matrix)
    grid_jv = grid[row_asses[:len(poses)]]
    #grid_jv = grid[row_asses]
    return np.round(grid_jv*(out_dim-1)).astype(np.int)
    #out = np.ones((out_dim*out_res, out_dim*out_res, 3))
def umap_grid(distance_matrix, padding_ratio=3/2):
    v = np.minimum(MAX, distance_matrix)
    n = np.square(int(np.ceil(np.sqrt(v.shape[0])))*padding_ratio)
    v = np.pad(v, ((0,n-v.shape[0]), (0, n-v.shape[0])), mode='constant', constant_values=MAX)
    v = UMAP.UMAP(metric="precomputed").fit_transform(v)
    v -= v.min(axis=0)
    v /= v.max(axis=0)
    out_dim = int(np.sqrt(n))
    grid = np.dstack(np.meshgrid(np.linspace(0, 1, out_dim), np.linspace(0, 1, out_dim))).reshape(-1, 2)
    #grid = np.dstack(np.meshgrid(np.linspace(0, out_dim-1, out_dim), np.linspace(0, out_dim-1, out_dim))).reshape(-1, 2)
    cost_matrix = cdist(v, grid, "sqeuclidean").astype(np.float32)
    cost_matrix = cost_matrix * (100000 / cost_matrix.max())
    row_asses, col_asses, _ = lapjv(cost_matrix)
    grid_jv = grid[row_asses]
    return np.round(grid_jv*(out_dim-1)).astype(np.int)
    #out = np.ones((out_dim*out_res, out_dim*out_res, 3))

def tsne_grid(similarity_matrix, max_iter=1000):
    v = similarity_matrix
    n = np.square(int(np.ceil(np.sqrt(v.shape[0]))))
    v = np.pad(v, ((0,n-v.shape[0]), (0, n-v.shape[0])), mode='constant', constant_values=0)
    v = TSNE(v, no_dims=2, max_iter=max_iter)
    v -= v.min(axis=0)
    v /= v.max(axis=0)
    out_dim = int(np.sqrt(n))
    grid = np.dstack(np.meshgrid(np.linspace(0, 1, out_dim), np.linspace(0, 1, out_dim))).reshape(-1, 2)
    #grid = np.dstack(np.meshgrid(np.linspace(0, out_dim-1, out_dim), np.linspace(0, out_dim-1, out_dim))).reshape(-1, 2)
    cost_matrix = cdist(v, grid, "sqeuclidean").astype(np.float32)
    cost_matrix = cost_matrix * (100000 / cost_matrix.max())
    row_asses, col_asses, _ = lapjv(cost_matrix)
    grid_jv = grid[row_asses]
    return np.round(grid_jv*(out_dim-1)).astype(np.int)
    #out = np.ones((out_dim*out_res, out_dim*out_res, 3))

'''
def grid(similarity_matrix, images, output_img_size):
    v = TSNE(similarity_matrix, no_dims=2)
    v -= v.min(axis=0)
    v /= v.max(axis=0)
    each_image_size = si
    ratio = int(output_img_size / each_img_size)
    tsne_norm = self.projection_vectors[:, ] # / ratio
    used_imgs = np.equal(self.projection_vectors[:, 0], None)
    image = np.ones((self.output_img_size, self.output_img_size, 3)) * self.background_color
    for x in tqdm(range(ratio)):
        x0 = x * self.each_img_size
        x05 = (x + 0.5) * self.each_img_size
        y = 0
        while y < ratio:
            y0 = y * self.each_img_size
            y05 = (y + 0.5) * self.each_img_size
            tmp_tsne = tsne_norm - [x05, y05]
            tmp_tsne[used_imgs] = 99999  # don't use the same img twice
            tsne_dist = np.hypot(tmp_tsne[:, 0], tmp_tsne[:, 1])
            min_index = np.argmin(tsne_dist)
            used_imgs[min_index] = True
            img_path = self.image_list[min_index]
            small_img, x1, y1, dx, dy = get_image(img_path, self.each_img_size)
            if small_img is None:
                continue
            if x < 1 and all(side < self.each_img_size for side in [x1, y1]):
                self.each_img_size = min(x1, y1)
                dx = int(ceil(x1 / 2))
                dy = int(ceil(y1 / 2))
            image[y0 + dy:y0 + dy + y1,x0 + dx:x0 + dx + x1] = small_img
            y += 1
    return image
'''

def h_beta(d=np.array([]), beta=1.0):
    """Compute the perplexity and the P-row for a specific value of the precision of a Gaussian distribution."""

    # Compute P-row and corresponding perplexity
    p = np.exp(-d.copy() * beta)
    sum_p = sum(p) + 1

    h = np.log(sum_p) + beta * np.sum(d * p) / sum_p
    p /= sum_p
    return h, p


def TSNE(similarity_matrix, no_dims=2, max_iter=1000):
    n, _ = similarity_matrix.shape
    initial_momentum = 0.5
    final_momentum = 0.8
    eta = 500
    min_gain = 0.01
    y = np.random.randn(n, no_dims)
    dy = np.zeros((n, no_dims))
    iy = np.zeros((n, no_dims))
    gains = np.ones((n, no_dims))

    # Compute P-values
    #p = x2p(x, 1e-5, perplexity)
    p = similarity_matrix
    p += np.transpose(p)
    p = p/np.sum(p)
    p *= 4									# early exaggeration
    p = np.maximum(p, 1e-12)
    c = j = 0
    # Run iterations
    for j in tqdm(range(max_iter)):

        # Compute pairwise affinities
        sum_y = np.sum(np.square(y), 1)
        num = 1 / (1 + np.add(np.add(-2 * np.dot(y, y.T), sum_y).T, sum_y))
        num[range(n), range(n)] = 0
        q = num / np.sum(num)
        q = np.maximum(q, 1e-12)

        # Compute gradient
        pq = p - q
        for i in range(n):
            dy[i, :] = np.sum(np.tile(pq[:, i] * num[:, i], (no_dims, 1)).T * (y[i, :] - y), 0)

        # Perform the update
        if j < 20:
            momentum = initial_momentum
        else:
            momentum = final_momentum
        gains = (gains + 0.2) * ((dy > 0) != (iy > 0)) + (gains * 0.8) * ((dy > 0) == (iy > 0))
        gains[gains < min_gain] = min_gain
        iy = momentum * iy - eta * (gains * dy)
        y += iy
        y -= np.tile(np.mean(y, 0), (n, 1))

        # Stop lying about P-values
        if j == 100:
            p /= 4

        c = np.sum(p * np.log(p / q))
    print("After {} Iterations the error is {}".format(j + 1, c))

    return y

def test():
    sim = np.array([
        [0, 99, 0, 0, 0],
        [99, 0, 0, 0, 0],
        [0, 0, 0, 0, 0],
        [0, 0, 0, 0, 1],
        [0, 0, 0, 1, 0],
        ])
    poses = umap(sim)
    print(poses)

    print(grid(poses))


if __name__ == "__main__":
    test()

